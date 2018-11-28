package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"

	"encoding/binary"

	"time"

	"github.com/boltdb/bolt"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/consensus/raftgroup"
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/logstore"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/jsonutil"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server/adminapi/apiclient"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/server/adminapi/apiserver"
	"github.com/vipshop/vdl/stablestore"
	"google.golang.org/grpc"
)

const (
	MetaFileSuffix = "_snapshot_meta.json"
)

const (
	LogDir = "restore_log"
)

const (
	DirPermission  = 0700
	FilePermission = 0600
)

const (
	NewNode = "new"
	OldNode = "old"
)

const (
	MoveType = "mv"
	CopyType = "cp"
)

type Restorer struct {
	DataDir         string
	StableStoreDir  string
	VdlAdminUrl     string
	RestoreDataMode string
	SnapDir         string
	LogDir          string
	RaftGroup       string
	NodeStatus      string
	ServerName      string
	PeerURL         string
	SnapMeta        *apiserver.SnapshotMeta
}

type RestorerConfig struct {
	DataDir         string
	StableStoreDir  string
	VdlAdminUrl     string
	RestoreDataMode string
	SnapDir         string
	NodeStatus      string
	ServerName      string
	RaftGroup       string
	PeerURL         string
}

func NewRestorer(conf *RestorerConfig) *Restorer {
	var err error
	s := new(Restorer)
	s.DataDir = conf.DataDir
	s.StableStoreDir = conf.StableStoreDir
	s.VdlAdminUrl = conf.VdlAdminUrl
	s.RestoreDataMode = conf.RestoreDataMode
	s.SnapDir = conf.SnapDir
	s.LogDir = path.Join(s.SnapDir, LogDir)
	s.RaftGroup = conf.RaftGroup
	s.NodeStatus = conf.NodeStatus
	s.ServerName = conf.ServerName
	s.PeerURL = conf.PeerURL

	metaFilePath := path.Join(s.SnapDir, s.RaftGroup+MetaFileSuffix)
	s.SnapMeta, err = parseSnapMeta(metaFilePath)
	if err != nil {
		glog.Fatalf("parseSnapMeta error,filePath=%s", metaFilePath)
	}

	if s.SnapMeta.RaftGroupName != s.RaftGroup {
		glog.Fatalf("raft group not match,s.SnapMeta.RaftGroupName=%s,s.RaftGroup=%s",
			s.SnapMeta.RaftGroupName, s.RaftGroup)
	}
	return s
}

func (s *Restorer) Start() error {
	//初始化log
	glog.InitGlog(s.LogDir, true)
	defer glog.Flush()
	err := s.RestoreStableStore()
	if err != nil {
		glog.Errorf("RestoreStableStore error,err=%s", err.Error())
		return err
	}
	err = s.RestoreData()
	if err != nil {
		glog.Errorf("RestoreData error,err=%s", err.Error())
		return err
	}
	return nil
}

func (s *Restorer) RestoreStableStore() error {
	err := s.checkAndMkDir()
	if err != nil {
		glog.Errorf("checkAndMkDir error,err=%s", err.Error())
		return err
	}

	dbFilePath := path.Join(s.StableStoreDir, stablestore.VDLMetadataFile)
	db, err := bolt.Open(dbFilePath, FilePermission, nil)
	if err != nil {
		glog.Errorf("Open in RestoreStableStore error,err=%s,path=%s", err.Error(), dbFilePath)
		return err
	}
	defer db.Close()
	err = s.setApplyIndex(db)
	if err != nil {
		glog.Errorf("setApplyIndex error,err=%s", err.Error())
		return err
	}
	err = s.setVoteFor(db)
	if err != nil {
		glog.Errorf("setVoteFor error,err=%s", err.Error())
		return err
	}
	err = s.setMemberships(db)
	if err != nil {
		glog.Errorf("setMemberships error,err=%s", err.Error())
		return err
	}
	err = s.setMembershipMeta(db)
	if err != nil {
		glog.Errorf("setMembershipMeta error,err=%s", err.Error())
		return err
	}
	return nil
}

func (s *Restorer) checkAndMkDir() error {
	IsExist, err := pathExists(s.StableStoreDir)
	if err != nil {
		glog.Errorf("pathExists error,err=%s,path=%s", err.Error(), s.StableStoreDir)
		return err
	}
	if s.NodeStatus == NewNode {
		if IsExist {
			err = os.RemoveAll(s.StableStoreDir)
			if err != nil {
				return err
			}
		}
		err = os.MkdirAll(s.StableStoreDir, DirPermission)
		if err != nil {
			return err
		}
	} else {
		if IsExist == false {
			glog.Errorf("dir not exist in old node,dir=%s", s.StableStoreDir)
			return errFileNotExist
		}
		dbFilePath := path.Join(s.StableStoreDir, stablestore.VDLMetadataFile)
		isDBExist, err := pathExists(dbFilePath)
		if err != nil {
			glog.Errorf("pathExists error,err=%s,path=%s", err.Error(), dbFilePath)
			return err
		}
		if isDBExist == false {
			glog.Errorf("db file not exist in old node,filePath=%s", dbFilePath)
			return errFileNotExist
		}
	}
	return nil
}

func (s *Restorer) RestoreData() error {
	IsExist, err := pathExists(s.DataDir)
	if err != nil {
		glog.Errorf("pathExists error,err=%s,path=%s", err.Error(), s.DataDir)
		return err
	}
	if IsExist {
		err = os.RemoveAll(s.DataDir)
		if err != nil {
			return err
		}
	}
	err = os.MkdirAll(s.DataDir, DirPermission)
	if err != nil {
		return err
	}

	err = s.buildData()
	if err != nil {
		glog.Errorf("build data error,err=%s", err.Error())
		return err
	}

	err = s.buildSegmentRangeMeta()
	if err != nil {
		glog.Errorf("buildSegmentRangeMeta error,err=%s", err.Error())
		return err
	}

	err = s.buildExistFlagFile()
	if err != nil {
		glog.Errorf("buildExistFlagFile error,err=%s", err.Error())
		return err
	}
	return nil
}

func (s *Restorer) setApplyIndex(db *bolt.DB) error {
	meta := s.SnapMeta
	applyIndexKey := raftgroup.ApplyIndexKeyPrefix + meta.RaftGroupName
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, meta.ApplyIndex)

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(stablestore.BucketName))
		if err != nil {
			glog.Fatalf("CreateBucketIfNotExists error:error=%s,BucketName=%s",
				err.Error(), stablestore.BucketName)
			return err
		}

		err = b.Put([]byte(applyIndexKey), buf)
		if err != nil {
			glog.Fatalf("Put in setApplyIndex error,err=%s,applyIndexKey=%s,value=%d",
				err.Error(), applyIndexKey, meta.ApplyIndex)
			return err
		}
		return nil
	})
}

func (s *Restorer) setVoteFor(db *bolt.DB) error {
	meta := s.SnapMeta
	voteForKey := []byte(raftgroup.VoteKeyPrefix + meta.RaftGroupName)
	newVoteFor := raftgroup.PersistentVote{
		Term: meta.Vote.Term,
		Vote: 0,
	}

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(stablestore.BucketName))
		if err != nil {
			glog.Fatalf("CreateBucketIfNotExists error:error=%s,BucketName=%s",
				err.Error(), stablestore.BucketName)
			return err
		}
		oldVoteForValue := b.Get(voteForKey)
		//key存在
		if 0 < len(oldVoteForValue) {
			var oldVoteFor raftgroup.PersistentVote
			jsonutil.MustUnMarshal(oldVoteForValue, &oldVoteFor)
			//term相等时，不设置vote for
			if oldVoteFor.Term == newVoteFor.Term {
				glog.Infof("oldVoteFor.Term[%d] is equal newVoteFor[%d]", oldVoteFor.Term, newVoteFor.Term)
				return nil
			} else if newVoteFor.Term < oldVoteFor.Term {
				//已经存在的term大于snapshot中的term
				//这种情况有可能出现在网络分区，term不断增加，该snapshot恢复的节点加入cluster后会触发cluster重新选主
				glog.Infof("oldVoteFor.Term[%d] is large than newVoteFor[%d]", oldVoteFor.Term, newVoteFor.Term)
				return nil
			}
		}
		//key不存在
		//key中的Term小于snapshot中的term
		err = b.Put(voteForKey, jsonutil.MustMarshal(newVoteFor))
		if err != nil {
			glog.Fatalf("Put in setVoteFor error,err=%s,voteForKey=%s,value=%v",
				err.Error(), voteForKey, newVoteFor)
		}
		return nil
	})
}

func (s *Restorer) setMemberships(db *bolt.DB) error {
	meta := s.SnapMeta
	members, err := s.memberListInCluster()
	if err != nil {
		return err
	}

	if s.NodeStatus == NewNode {
		equal := s.checkMemberships(members)
		if equal {
			err = s.addNewNodeMembership()
			if err != nil {
				return err
			}
		} else {
			equalWithNew := s.checkMembershipsWithNewNode(members, s.ServerName)
			if equalWithNew == false {
				glog.Fatalf("restore membership fail,membership not equal,members=%v,snapMembership=%v",
					members, meta.Members)
			}
		}
	} else {
		equal := s.checkMemberships(members)
		if equal == false {
			glog.Fatalf("restore membership fail,membership not equal,members=%v,snapMembership=%v",
				members, meta.Members)
		}
	}
	msOnline, err := s.memberListInCluster()
	if err != nil {
		return err
	}

	membershipKey := membership.MembersKeyPrefix + meta.RaftGroupName
	msBuf := jsonutil.MustMarshal(msOnline)
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(stablestore.BucketName))
		if err != nil {
			glog.Fatalf("CreateBucketIfNotExists error:error=%s,BucketName=%s",
				err.Error(), stablestore.BucketName)
			return err
		}
		err = b.Put([]byte(membershipKey), msBuf)
		if err != nil {
			glog.Fatalf("Put in setMemberships error,err=%s,membershipKey=%s,value=%v",
				err.Error(), membershipKey, *msOnline)
			return err
		}
		return nil
	})
}

func (s *Restorer) setMembershipMeta(db *bolt.DB) error {
	meta := s.SnapMeta
	msMeta, err := s.getMembershipsMetaByName(s.ServerName)
	if err != nil {
		glog.Errorf("getMembershipsMetaByName error,err=%s,serverName=%s", err.Error(), s.ServerName)
		return err
	}
	key := membership.MetaKeyPrefix + meta.RaftGroupName
	buf := jsonutil.MustMarshal(msMeta)

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(stablestore.BucketName))
		if err != nil {
			glog.Fatalf("CreateBucketIfNotExists error:error=%s,BucketName=%s",
				err.Error(), stablestore.BucketName)
			return err
		}
		err = b.Put([]byte(key), buf)
		if err != nil {
			glog.Fatalf("Put in setMemberships error,err=%s,membershipKey=%s,value=%v",
				err.Error(), key, *msMeta)
			return err
		}
		return nil
	})
}

func (s *Restorer) checkMemberships(ms *membership.MembershipMembers) bool {
	snapMs := s.SnapMeta.Members
	if len(snapMs.MembersMap) == len(ms.MembersMap) {
		for id, _ := range snapMs.MembersMap {
			if _, ok := ms.MembersMap[id]; ok == false {
				return false
			}
		}
	} else {
		return false
	}

	if len(snapMs.RemovedMap) == len(ms.RemovedMap) {
		for id, _ := range snapMs.RemovedMap {
			if _, ok := ms.RemovedMap[id]; ok == false {
				return false
			}
		}
	} else {
		return false
	}

	return true
}

func (s *Restorer) checkMembershipsWithNewNode(ms *membership.MembershipMembers, newServerName string) bool {
	snapMs := s.SnapMeta.Members

	for id, _ := range snapMs.RemovedMap {
		if _, ok := ms.RemovedMap[id]; ok == false {
			return false
		}
	}

	for id, _ := range snapMs.MembersMap {
		if _, ok := ms.MembersMap[id]; ok == false {
			return false
		}
	}

	if len(snapMs.MembersMap)+1 == len(ms.MembersMap) {
		for _, v := range ms.MembersMap {
			if v.Name == newServerName {
				return true
			}
		}
	} else {
		return false
	}

	return false
}

func (s *Restorer) memberListInCluster() (*membership.MembershipMembers, error) {
	client, err := apiclient.NewClient([]string{s.VdlAdminUrl}, 5*time.Second)
	if err != nil {
		glog.Errorf("New Client error,err=%s", err.Error())
		return nil, err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, errList := client.MemberList(ctx, &apipb.MemberListRequest{
		LogstreamName: s.SnapMeta.RaftGroupName,
	}, grpc.FailFast(false))

	cancel()

	if errList != nil {
		glog.Errorf("Member List error,err=%s", errList.Error())
		return nil, errList
	}
	ms := &membership.MembershipMembers{
		MembersMap: make(map[types.ID]*membership.Member),
		RemovedMap: make(map[types.ID]bool),
	}

	for _, v := range resp.RemovedMembers {
		ms.RemovedMap[types.ID(v)] = true
	}
	for _, v := range resp.Members {
		id := types.ID(v.ID)
		member := &membership.Member{
			ID:       id,
			Name:     v.Name,
			PeerURLs: v.PeerURLs,
		}
		ms.MembersMap[id] = member
	}
	return ms, nil
}

func (s *Restorer) getMembershipsMetaByName(serverName string) (*membership.MembershipMeta, error) {
	client, err := apiclient.NewClient([]string{s.VdlAdminUrl}, 5*time.Second)
	if err != nil {
		glog.Errorf("New Client error,err=%s", err.Error())
		return nil, err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, errList := client.MemberList(ctx, &apipb.MemberListRequest{
		LogstreamName: s.SnapMeta.RaftGroupName,
	}, grpc.FailFast(false))

	cancel()

	if errList != nil {
		glog.Errorf("Member List error,err=%s", errList.Error())
		return nil, errList
	}

	msMeta := membership.MembershipMeta{}
	msMeta.Version = membership.MembershipVersion(0)
	msMeta.ClusterID = types.ID(resp.Header.ClusterId)
	//not set self member id
	msMeta.SelfMemberID = types.ID(0)

	return &msMeta, nil
}

func (s *Restorer) addNewNodeMembership() error {
	serverName := s.ServerName
	raftGroup := s.SnapMeta.RaftGroupName

	client, err := apiclient.NewClient([]string{s.VdlAdminUrl}, 5*time.Second)
	if err != nil {
		glog.Errorf("New Client error,err=%s", err.Error())
		return err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, errAdd := client.MemberAdd(ctx, &apipb.MemberAddRequest{
		ServerName:    serverName,
		LogstreamName: raftGroup,
		PeerURLs:      []string{s.PeerURL},
	}, grpc.FailFast(false))

	cancel()

	if errAdd != nil {
		glog.Errorf("Member Add error,err=%s", errAdd.Error())
		return errAdd
	}

	return nil
}

func (s *Restorer) buildData() error {
	var err error
	for _, segment := range s.SnapMeta.Segments {
		srcSegmentPath := path.Join(s.SnapDir, segment.SegmentName)
		dstSegmentPath := path.Join(s.DataDir, segment.SegmentName)
		if s.RestoreDataMode == MoveType {
			err = moveFile(srcSegmentPath, dstSegmentPath)
			if err != nil {
				glog.Errorf("moveFile error,err=%s,srcSegment=%s,dstSegment=%s",
					err.Error(), srcSegmentPath, dstSegmentPath)
				return err
			}
		} else if s.RestoreDataMode == CopyType {
			err = copyFile(srcSegmentPath, dstSegmentPath)
			if err != nil {
				glog.Errorf("copyFile error,err=%s,srcSegment=%s,dstSegment=%s",
					err.Error(), srcSegmentPath, dstSegmentPath)
				return err
			}
		}

		srcIndexPath := path.Join(s.SnapDir, segment.Index.IndexName)
		dstIndexPath := path.Join(s.DataDir, segment.Index.IndexName)
		if s.RestoreDataMode == MoveType {
			err = moveFile(srcIndexPath, dstIndexPath)
			if err != nil {
				glog.Errorf("moveFile error,err=%s,srcIndex=%s,dstIndex=%s",
					err.Error(), srcIndexPath, dstIndexPath)
				return err
			}
		} else if s.RestoreDataMode == CopyType {
			err = copyFile(srcIndexPath, dstIndexPath)
			if err != nil {
				glog.Errorf("copyFile error,err=%s,srcIndex=%s,dstIndex=%s",
					err.Error(), srcIndexPath, dstIndexPath)
				return err
			}
		}
		glog.Infof("restore segment[%s] and index[%s] success", segment.SegmentName, segment.Index.IndexName)
	}

	return nil
}

func (s *Restorer) buildSegmentRangeMeta() error {
	rangeFile := logstore.NewRangeFile(s.DataDir)
	//追加除最后一个segment以外的全部范围
	for i := 0; i < len(s.SnapMeta.Segments)-1; i++ {
		segment := s.SnapMeta.Segments[i]
		logRange := &logstore.LogRange{
			FirstRindex: segment.FirstRindex,
			LastRindex:  segment.LastRindex,
			FirstVindex: segment.FirstVindex,
			LastVindex:  segment.LastVindex,
		}
		err := rangeFile.AppendLogRange(segment.SegmentName, logRange)
		if err != nil {
			glog.Errorf("AppendLogRange error,err=%s,segment=%s,logRange=%v",
				err.Error(), segment.SegmentName, *logRange)
			return err
		}
	}
	glog.Infof("build segment range metadata success,dir=%s", s.DataDir)
	return nil
}

func (s *Restorer) buildExistFlagFile() error {
	exitFile := logstore.NewFlagFile(s.DataDir)
	exitFile.WriteExitFlag(logstore.UnnormalExitFlag)
	glog.Infof("build exit flag file success,dir=%s", s.DataDir)
	return nil
}

func parseSnapMeta(metaFilePath string) (*apiserver.SnapshotMeta, error) {
	var meta apiserver.SnapshotMeta
	isExist, err := pathExists(metaFilePath)
	if err != nil {
		return nil, err
	}
	if isExist {
		data, err := ioutil.ReadFile(metaFilePath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, err
		}
	} else {
		glog.Errorf("snap meta file not exist,filePath=%s", metaFilePath)
		return nil, errFileNotExist
	}
	return &meta, nil
}

func pathExists(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func copyFile(srcPath, dstPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	fileInfo, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	maxFileSize := fileInfo.Size()

	dstFile, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, FilePermission)
	if err != nil {
		glog.Errorf("OpenFile error,err=%s,filePath=%s", dstPath)
		return err
	}
	defer dstFile.Close()

	//pre allocate maxFileSize space for this file
	if err = fileutil.Preallocate(dstFile, maxFileSize, true); err != nil {
		glog.Errorf("Preallocate error,err=%s,dstPath=%s", err.Error(), dstPath)
		return err
	}

	io.Copy(dstFile, srcFile)

	return nil
}

func moveFile(srcPath, dstPath string) error {
	isExist, err := pathExists(srcPath)
	if err != nil {
		return err
	}
	if isExist == false {
		glog.Errorf("srcPath not exist,path=%s", srcPath)
		return errFileNotExist
	}

	err = os.Rename(srcPath, dstPath)
	if err != nil {
		return err
	}
	return nil
}
