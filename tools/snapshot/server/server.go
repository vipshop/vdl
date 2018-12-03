package main

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"time"

	"os"

	"path"

	"crypto/md5"

	"strconv"

	"encoding/json"

	"path/filepath"

	"encoding/hex"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/juju/ratelimit"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/adminapi/apiclient"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/server/adminapi/apiserver"
	"google.golang.org/grpc"
)

const (
	MetaFileSuffix = "_snapshot_meta.json"
	MetaDirName    = "meta"
	TmpDirName     = "tmp"
	LogDirName     = "log"
)

const (
	DirPermission      = 0755
	MetaFilePermission = 0644
)

const (
	TempSegmentDirPrefix = "segment"
	TempIndexDirPrefix   = "index"
)

const (
	rateNoLimit int64 = 1024
)

type SnapSendServer struct {
	Addr        string
	VdlAdminUrl string
	DataDir     string
	MetaDir     string
	LogDir      string
	TmpDir      string
	SnapMeta    *apiserver.SnapshotMeta
	RateBucket  *ratelimit.Bucket
}

type ServerConfig struct {
	Addr        string
	VdlAdminUrl string
	DataDir     string
	BaseDir     string
	RateLimit   int
}

func NewSnapSendServer(conf *ServerConfig) (*SnapSendServer, error) {
	s := new(SnapSendServer)

	s.Addr = conf.Addr
	s.VdlAdminUrl = conf.VdlAdminUrl
	s.DataDir = conf.DataDir
	s.MetaDir = path.Join(conf.BaseDir, MetaDirName)
	s.LogDir = path.Join(conf.BaseDir, LogDirName)
	s.TmpDir = path.Join(conf.BaseDir, TmpDirName)

	//check snapshot metadata dir, if not exist,create dir
	isExist, err := pathExists(s.MetaDir)
	if err != nil {
		return nil, err
	}
	if isExist == false {
		err := os.MkdirAll(s.MetaDir, DirPermission)
		if err != nil {
			return nil, err
		}
	}

	isExist, err = pathExists(s.LogDir)
	if err != nil {
		return nil, err
	}
	if isExist == false {
		err := os.MkdirAll(s.LogDir, DirPermission)
		if err != nil {
			return nil, err
		}
	}

	isExist, err = pathExists(s.TmpDir)
	if err != nil {
		return nil, err
	}
	if isExist == true {
		err := os.RemoveAll(s.TmpDir)
		if err != nil {
			return nil, err
		}
	}
	err = os.MkdirAll(s.TmpDir, 0755)
	if err != nil {
		return nil, err
	}

	s.SnapMeta = nil

	var rate int64
	if conf.RateLimit == 0 {
		rate = rateNoLimit
	} else {
		rate = int64(conf.RateLimit)
	}
	s.RateBucket = ratelimit.NewBucketWithRate(float64(rate*1024*1024), rate*1024*1024)

	return s, nil
}

func (s *SnapSendServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/get_snap", s.getSnapMetaFromVDL)
	mux.HandleFunc("/api/get_index", s.getIndexFromVDL)
	mux.HandleFunc("/api/get_segment", s.getSegmentFromVDL)

	//初始化log
	glog.InitGlog(s.LogDir, true)
	defer glog.Flush()
	err := http.ListenAndServe(s.Addr, mux)
	if err != nil {
		glog.Errorf("ListenAndServe error,err=%s", err.Error())
		return err
	}
	return nil
}

func (s *SnapSendServer) getSnapMetaFromVDL(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	//check method
	if r.Method != "GET" {
		glog.Errorf("request method is not GET,r.Method=%s", r.Method)
		http.Error(w, "only support GET method", http.StatusBadRequest)
		return
	}
	raftGroup := r.Form.Get("raft_group")
	if len(raftGroup) == 0 {
		glog.Errorf("raft group is nil,r.URL=%s", r.URL.String())
		http.Error(w, "raft group is nil", http.StatusNotFound)
		return
	}
	//generate meta file
	err := s.generateMetaFile(raftGroup)
	if err != nil {
		glog.Errorf("generateMetaFile error,err=%s,raftGroup=%s", err.Error(), raftGroup)
		http.Error(w, "generateMetaFile error:"+err.Error(), http.StatusInternalServerError)
		return
	}

	metaFilePath := path.Join(s.MetaDir, raftGroup+MetaFileSuffix)
	s.SnapMeta, err = s.getSnapMeta(metaFilePath)
	if err != nil {
		glog.Errorf("getSnapMeta error,err=%s,metaFilePath=%s", err.Error(), metaFilePath)
		http.Error(w, "getSnapMeta error:"+err.Error(), http.StatusInternalServerError)
		return
	}

	//send file
	err = s.sendFile(w, metaFilePath)
	if err != nil {
		glog.Errorf("sendFile Error,err=%s,metaFilePath=%s", err.Error(), metaFilePath)
		http.Error(w, "sendFile error:"+err.Error(), http.StatusInternalServerError)
		return
	}
	glog.Infof("send snapshot meta file[%s] to %s successfully", raftGroup+MetaFileSuffix, r.RemoteAddr)
	return
}

func (s *SnapSendServer) getSegmentFromVDL(w http.ResponseWriter, r *http.Request) {
	var err error
	//raft group
	r.ParseForm()
	//check method
	if r.Method != "GET" {
		glog.Errorf("request method is not GET,r.Method=%s", r.Method)
		http.Error(w, "only support GET method", http.StatusBadRequest)
		return
	}

	raftGroup := r.Form.Get("raft_group")
	if len(raftGroup) == 0 {
		glog.Errorf("raft group is nil,r.URL=%s", r.URL.String())
		http.Error(w, "raft group is nil", http.StatusBadRequest)
		return
	}

	// 判断是否是最后一个segment，由客户端上传标志
	str := r.Form.Get("is_last_segment")
	if len(str) == 0 {
		glog.Errorf("is_last_segment is nil,r.URL=%s", r.URL.String())
		http.Error(w, "is_last_segment is nil", http.StatusBadRequest)
		return
	}

	//
	segmentName := r.Form.Get("segment_name")
	if len(segmentName) == 0 {
		glog.Errorf("segmentName is nil,r.URL=%s", r.URL.String())
		http.Error(w, "segmentName is nil", http.StatusBadRequest)
		return
	}

	isLastSegment, err := strconv.ParseBool(str)
	if err != nil {
		glog.Errorf("strconv is_last_segment error,r.URL=%s", r.URL.String())
		http.Error(w, "strconv is_last_segment error", http.StatusBadRequest)
		return
	}

	var segmentPath string
	if isLastSegment {
		segmentPath, err = s.generateLastSegment(segmentName)
		if err != nil {
			glog.Errorf("generateLastSegment error,err=%s,segmentName=%s", err.Error(), segmentName)
			http.Error(w, "generateLastSegment error:"+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		segmentPath = path.Join(s.DataDir, segmentName)
	}

	//send file
	err = s.sendFile(w, segmentPath)
	if err != nil {
		glog.Errorf("sendFile Error,err=%s,segmentPath=%s", err.Error(), segmentPath)
		http.Error(w, "sendFile error:"+err.Error(), http.StatusInternalServerError)
		return
	}
	glog.Infof("send segment file[%s] to %s successfully", segmentName, r.RemoteAddr)
	return
}

func (s *SnapSendServer) getIndexFromVDL(w http.ResponseWriter, r *http.Request) {
	var err error
	r.ParseForm()
	//check method
	if r.Method != "GET" {
		glog.Errorf("request method is not Get,r.Method=%s", r.Method)
		http.Error(w, "only support GET method", http.StatusBadRequest)
		return
	}
	raftGroup := r.Form.Get("raft_group")
	if len(raftGroup) == 0 {
		glog.Errorf("raft group is nil,r.URL=%s", r.URL.String())
		http.Error(w, "raft group is nil", http.StatusBadRequest)
	}

	indexName := r.Form.Get("index_name")
	if len(indexName) == 0 {
		glog.Errorf("indexName is nil,r.URL=%s", r.URL.String())
		http.Error(w, "indexName is nil", http.StatusBadRequest)
		return
	}

	str := r.Form.Get("is_last_index")
	if len(str) == 0 {
		glog.Errorf("is_last_index is nil,r.URL=%s", r.URL.String())
		http.Error(w, "is_last_index is nil", http.StatusBadRequest)
		return
	}

	isLastIndex, err := strconv.ParseBool(str)
	if err != nil {
		glog.Errorf("strconv is_last_index error,r.URL=%s", r.URL.String())
		http.Error(w, "strconv is_last_index error", http.StatusBadRequest)
		return
	}

	var indexPath string
	if isLastIndex {
		indexPath, err = s.generateLastIndex(indexName)
		if err != nil {
			glog.Errorf("generateLastIndex error,err=%s,segmentName=%s", err.Error(), indexName)
			http.Error(w, "generateLastIndex error:"+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		indexPath = path.Join(s.DataDir, indexName)
	}

	err = s.sendFile(w, indexPath)
	if err != nil {
		glog.Errorf("sendFile Error,err=%s,indexPath=%s", err.Error(), indexPath)
		http.Error(w, "sendFile error:"+err.Error(), http.StatusInternalServerError)
		return
	}
	glog.Infof("send index file[%s] to %s successfully", indexName, r.RemoteAddr)
	return
}

func (s *SnapSendServer) generateMetaFile(raftGroup string) error {
	client, err := apiclient.NewClient([]string{s.VdlAdminUrl}, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := client.CreateSnapshotMeta(
		ctx,
		&apipb.SnapshotRequeest{RaftGroup: raftGroup},
		grpc.FailFast(false),
	)
	cancel()
	if err != nil {
		return err
	}

	if resp.ResultStatus == "OK" {
		err := s.writeIntoFile(resp.Msg, raftGroup+MetaFileSuffix)
		if err != nil {
			glog.Errorf("writeIntoFile error,err=%s,resp.Msg=%s", err.Error(), string(resp.Msg))
			return err
		}
		return nil
	}
	return errors.New(string(resp.Msg))
}

func (s *SnapSendServer) writeIntoFile(buf []byte, fileName string) error {
	if len(buf) == 0 {
		return nil
	}

	filePath := path.Join(s.MetaDir, fileName)
	err := ioutil.WriteFile(filePath, buf, MetaFilePermission)
	if err != nil {
		return err
	}
	return nil
}

//在s.TmpDir目录下生成一个tmp目录，并在目录下面生成lastSegment文件
func (s *SnapSendServer) generateLastSegment(segmentName string) (string, error) {
	if s.SnapMeta == nil || len(s.SnapMeta.Segments) == 0 {
		return "", errSnapMeta
	}
	lastSegment := s.SnapMeta.Segments[len(s.SnapMeta.Segments)-1]
	if lastSegment.SegmentName != segmentName {
		return "", errNotMatch
	}

	tmpDir, err := ioutil.TempDir(s.TmpDir, TempSegmentDirPrefix)
	if err != nil {
		return "", err
	}
	dstPath := path.Join(tmpDir, segmentName)

	srcPath := path.Join(s.DataDir, segmentName)
	//last_position-0+1
	fileSize := lastSegment.EndPos + 1
	err = s.generateFile(dstPath, srcPath, fileSize, lastSegment.SegmentSizeBytes)
	if err != nil {
		glog.Errorf("generateFile,error,err=%s,dstPath=%s,srcPath=%s,fileSize=%d,maxSize=%d",
			err.Error(), dstPath, srcPath, lastSegment.EndPos, lastSegment.SegmentSizeBytes)
		return "", err
	}

	return dstPath, nil
}

func (s *SnapSendServer) generateLastIndex(indexName string) (string, error) {
	if s.SnapMeta == nil || len(s.SnapMeta.Segments) == 0 {
		return "", errSnapMeta
	}
	lastIndex := s.SnapMeta.Segments[len(s.SnapMeta.Segments)-1].Index
	if lastIndex.IndexName != indexName {
		return "", errNotMatch
	}

	tmpDir, err := ioutil.TempDir(s.TmpDir, TempIndexDirPrefix)
	if err != nil {
		return "", err
	}
	dstPath := path.Join(tmpDir, indexName)

	srcPath := path.Join(s.DataDir, indexName)
	fileSize := lastIndex.EndPos + 1
	err = s.generateFile(dstPath, srcPath, fileSize, fileSize)
	if err != nil {
		glog.Errorf("generateFile,error,err=%s,dstPath=%s,srcPath=%s,fileSize=%d,maxSize=%d",
			err.Error(), dstPath, srcPath, fileSize, fileSize)
		return "", err
	}

	return dstPath, nil
}

func (s *SnapSendServer) generateFile(dstPath string, srcPath string, fileSize int64, maxSize int64) error {
	isExist, err := pathExists(dstPath)
	if err != nil {
		glog.Errorf("pathExists error,err=%s,path=%s", err.Error(), dstPath)
		return err
	}
	if isExist {
		err = os.Remove(dstPath)
		if err != nil {
			glog.Errorf("Remove error,err=%s,path=%s", err.Error(), dstPath)
			return err
		}
	}

	dstFile, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		glog.Errorf("OpenFile error,err=%s,path=%s", err.Error(), dstPath)
		return err
	}
	defer dstFile.Close()

	//pre allocate maxSize space for this file
	if err = fileutil.Preallocate(dstFile, maxSize, true); err != nil {
		glog.Errorf("failed to allocate space when creating new segment file (%v)", err)
		return err
	}

	//open src with readonly
	srcFile, err := os.Open(srcPath)
	if err != nil {
		glog.Errorf("Open error,err=%s,path=%s", err.Error(), srcPath)
		return err
	}
	defer srcFile.Close()

	_, err = dstFile.Seek(0, io.SeekStart)
	if err != nil {
		glog.Errorf("Seek error,err=%s,fileName=%s", err.Error(), dstFile.Name())
		return err
	}
	io.CopyN(dstFile, srcFile, fileSize)
	return nil
}

func (s *SnapSendServer) getMd5(file *os.File) string {
	md5Value := md5.New()
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		glog.Fatalf("Seek error,err=%s,fileName=%s", err.Error(), file.Name())
	}
	io.Copy(md5Value, file)
	return hex.EncodeToString(md5Value.Sum(nil))
}

func (s *SnapSendServer) getFileSize(file *os.File) (int, error) {
	fileStat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fileStat.Size()), nil
}

func (s *SnapSendServer) getSnapMeta(filePath string) (*apiserver.SnapshotMeta, error) {
	metaBuf, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	snapMeta := new(apiserver.SnapshotMeta)
	err = json.Unmarshal(metaBuf, snapMeta)
	if err != nil {
		return nil, err
	}
	return snapMeta, nil
}

//send file
func (s *SnapSendServer) sendFile(w http.ResponseWriter, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		glog.Errorf("Open error,err=%s,filePath=%s", err.Error(), filePath)
		return err
	}
	defer file.Close()

	fileMd5 := s.getMd5(file)
	fileSize, err := s.getFileSize(file)
	if err != nil {
		glog.Errorf("getFileSize err,err=%s,fileName=%s", err.Error(), file.Name())
		return err
	}

	//set header
	w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(file.Name()))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(fileSize))
	w.Header().Set("File-Md5", fileMd5)

	//send file
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		glog.Errorf("Seek error,err=%s,fileName=%s", err.Error(), file.Name())
		return err
	}
	writeLen, err := io.Copy(w, ratelimit.Reader(file, s.RateBucket))
	if err != nil {
		glog.Errorf("Copy metafile error,err=%s,fileName=%s,writeLen=%d",
			err.Error(), file.Name(), writeLen)
		return err
	}

	return nil
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
