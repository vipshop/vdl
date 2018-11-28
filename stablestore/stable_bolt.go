// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package stablestore

import (
	"errors"
	"path"

	"os"

	"github.com/boltdb/bolt"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/vdlfiu"
	"gitlab.tools.vipshop.com/distributedstorage/fiu"
)


const (
	VDLMetadataFile = "vdl.db"
	BucketName      = "vdl_metadata"
)

type BoltDBStoreConfig struct {
	//boltdb dir
	Dir string
}

type BoltDBStore struct {
	Conf       *BoltDBStoreConfig
	DB         *bolt.DB
	BucketName []byte
}

func NewBoltDBStore(conf *BoltDBStoreConfig) (*BoltDBStore, error) {
	if conf == nil || len(conf.Dir) == 0 {
		return nil, errors.New("args not available")
	}
	//if dir not exist, make dir
	if isFileExist(conf.Dir) == false {
		err := os.MkdirAll(conf.Dir, 0700)
		if err != nil {
			return nil, err
		}
	}
	dbFilePath := path.Join(conf.Dir, VDLMetadataFile)
	db, err := bolt.Open(dbFilePath, 0600, nil)
	if err != nil {
		glog.Errorf("[stable_bolt.go-NewBoltDBStore]:bolt Open error,err=%s,dbpath=%s", err.Error(), dbFilePath)
		return nil, err
	}

	s := &BoltDBStore{
		Conf:       conf,
		DB:         db,
		BucketName: []byte(BucketName),
	}
	s.initialize()
	return s, nil
}

func (s *BoltDBStore) Put(key []byte, value []byte) error {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStableStoreWriteError) {
		return errors.New("Mock FiuStableStoreWriteError")
	}
	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStableStoreWriteDelay) {
		vdlfiu.SleepRandom(2000, 4000)
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		err := b.Put(key, value)
		if err != nil {
			glog.Fatalf("[stable_bolt.go-Put]::err=%s,key=%s", err.Error(), string(key))
		}
		return nil
	})
}

func (s *BoltDBStore) Get(key []byte) ([]byte, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStableStoreReadError) {
		return nil, errors.New("Mock FiuStableStoreReadError")
	}

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStableStoreReadDelay) {
		vdlfiu.SleepRandom(2000, 4000)
	}

	tx, err := s.DB.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(s.BucketName)
	val := bucket.Get(key)
	return val, nil
}

func (s *BoltDBStore) Close() error {
	s.DB.Close()
	return nil
}

func isFileExist(filePath string) bool {
	if len(filePath) == 0 {
		return false
	}
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		glog.Errorf("[stable_bolt.go-isFileExist]:stat error:error=%s,dir=%s", err.Error(), filePath)
		return false
	}
	return true
}

// initialize is used to set up all of the buckets.
func (s *BoltDBStore) initialize() error {
	tx, err := s.DB.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create bucket
	if _, err := tx.CreateBucketIfNotExists(s.BucketName); err != nil {
		glog.Fatalf("[stable_bolt.go-initialize]:CreateBucketIfNotExists error:error=%s,BucketName=%s",
			err.Error(), string(s.BucketName))
		return err
	}

	return tx.Commit()
}

func (s *BoltDBStore) GetBatch(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := s.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketName))
		for _, key := range keys {
			if len(key) != 0 {
				result[key] = bucket.Get([]byte(key))
			} else {
				glog.Warningf("GetBatch key is nil,keys=%v", keys)
			}
		}
		return nil
	})
	if err != nil {
		glog.Errorf("GetBatch error,err=%s,keys=%v", err.Error(), keys)
		return nil, err
	}
	return result, nil
}
