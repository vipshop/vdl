package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"path"

	"strconv"

	"mime"

	"crypto/md5"

	"encoding/hex"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/adminapi/apiserver"
)

const (
	DownloadSnapNewMode    = "new"
	DownloadSnapResumeMode = "resume"
)
const (
	SegmentFileType = "segment"
	IndexFileType   = "index"
	TmpFileType     = "tmp"
)

const (
	DirPermission  = 0700
	FilePermission = 0600
)

var (
	IndexFileSuffix   = ".idx"
	SegmentFileSuffix = ".log"
	TmpFileSuffix     = ".tmp"
)

const (
	LogDir = "download_log"
)

var (
	DownloadStatusFile = "download_status.txt"
	FailStatus         = "download_fail"
	SuccessStatus      = "download_success"
)

type SnapReceiveAgent struct {
	SnapDir    string
	ServerAddr string
	LogDir     string
	RaftGroup  string
	BootMode   string
	SnapMeta   *apiserver.SnapshotMeta
}

type AgentConfig struct {
	SnapDir    string
	ServerAddr string
	RaftGroup  string
	BootMode   string
}

type SnapFile struct {
	FileName string
	FileType string
	IsLast   bool
}

type FileInfo struct {
	FileName string
	CheckSum string
	FileSize int
}

func NewSnapReceiveAgent(conf *AgentConfig) (*SnapReceiveAgent, error) {
	s := new(SnapReceiveAgent)

	s.ServerAddr = conf.ServerAddr
	s.RaftGroup = conf.RaftGroup
	s.SnapDir = conf.SnapDir
	s.LogDir = path.Join(s.SnapDir, LogDir)
	s.BootMode = conf.BootMode
	s.SnapMeta = nil

	return s, nil
}

func (s *SnapReceiveAgent) Start() error {
	var err error
	switch s.BootMode {
	case DownloadSnapNewMode:
		err = s.GetSnapWithNew()
		if err != nil {
			glog.Errorf("GetSnapWithNew error,err=%s", err.Error())
			return err
		}
	case DownloadSnapResumeMode:
		err = s.GetSnapWithResume()
		if err != nil {
			glog.Errorf("GetSnapWithResume error,err=%s", err.Error())
			return err
		}
	default:
		glog.Errorf("boot mode not support,s.bootMode=%s", s.BootMode)
	}
	return nil
}

func (s *SnapReceiveAgent) GetSnapWithNew() error {
	snapDirIsExist, err := pathExists(s.SnapDir)
	if err != nil {
		return err
	}
	if snapDirIsExist == true {
		err = os.RemoveAll(s.SnapDir)
		if err != nil {
			return err
		}
	}
	err = os.MkdirAll(s.SnapDir, DirPermission)
	if err != nil {
		return err
	}
	//初始化log
	glog.InitGlog(s.LogDir, true)
	defer glog.Flush()

	//write download fail
	statusFilePath := path.Join(s.SnapDir, DownloadStatusFile)
	err = writeDownloadStatus(statusFilePath, FailStatus)
	if err != nil {
		glog.Errorf("writeDownloadStatus in GetSnapWithNew error,err=%s,filePath=%s,flag=%s",
			err.Error(), statusFilePath, FailStatus)
		return err
	}

	s.SnapMeta, err = s.GetSnapMeta()
	if err != nil {
		glog.Errorf("GetSnapMeta error,err=%s,SnapReciveAgent=%v", err.Error(), *s)
		return err
	}
	metaSnapFiles, err := s.GetSnapFilesFromMeta()
	if err != nil {
		glog.Errorf("GetSnapFilesFromMeta error,err=%s,SnapReciveAgent=%v", err.Error(), *s)
		return err
	}
	err = s.DownloadSnapFiles(metaSnapFiles)
	if err != nil {
		glog.Errorf("DownloadSnapFiles error,err=%s,SnapReciveAgent=%v", err.Error(), *s)
		return err
	}

	//write download success
	err = writeDownloadStatus(statusFilePath, SuccessStatus)
	if err != nil {
		glog.Errorf("writeDownloadStatus in GetSnapWithNew error,err=%s,filePath=%s,flag=%s",
			err.Error(), statusFilePath, SuccessStatus)
		return err
	}
	return nil
}

func (s *SnapReceiveAgent) GetSnapWithResume() error {
	snapDirIsExist, err := pathExists(s.SnapDir)
	if err != nil {
		return err
	}
	if snapDirIsExist == false {
		err = os.MkdirAll(s.SnapDir, DirPermission)
		if err != nil {
			return err
		}
	}
	//初始化log
	glog.InitGlog(s.LogDir, true)
	defer glog.Flush()

	//write download fail
	statusFilePath := path.Join(s.SnapDir, DownloadStatusFile)
	err = writeDownloadStatus(statusFilePath, FailStatus)
	if err != nil {
		glog.Errorf("writeDownloadStatus in  GetSnapWithResume error,err=%s,filePath=%s,flag=%s",
			err.Error(), statusFilePath, FailStatus)
		return err
	}

	s.SnapMeta, err = s.GetSnapMeta()
	if err != nil {
		glog.Errorf("GetSnapMeta error,err=%s", err.Error())
		return err
	}

	existSnapFiles, err := s.GetSnapFilesFromDir()
	if err != nil {
		glog.Errorf("GetSnapFilesFromDir error,err=%s", err.Error())
		return err
	}

	metaSnapFiles, err := s.GetSnapFilesFromMeta()
	if err != nil {
		glog.Errorf("GetSnapFilesFromMeta error,err=%s", err.Error())
		return err
	}
	//取交集
	reservedSnapFiles := sliceIntersect(existSnapFiles, metaSnapFiles)

	deleteSnapFiles := sliceDiff(existSnapFiles, reservedSnapFiles)
	for _, deleteFile := range deleteSnapFiles {
		filePath := path.Join(s.SnapDir, deleteFile.FileName)
		err := os.Remove(filePath)
		if err != nil {
			glog.Errorf("Remove file error,err=%s,filePath=%s", err.Error(), filePath)
			return err
		}
	}

	tranferFileList := sliceDiff(metaSnapFiles, reservedSnapFiles)
	err = s.DownloadSnapFiles(tranferFileList)
	if err != nil {
		glog.Errorf("DownloadSnapFiles error,err=%s", err.Error())
		return err
	}

	//write download success
	err = writeDownloadStatus(statusFilePath, SuccessStatus)
	if err != nil {
		glog.Errorf("writeDownloadStatus in GetSnapWithResume error,err=%s,filePath=%s,flag=%s",
			err.Error(), statusFilePath, SuccessStatus)
		return err
	}
	return nil
}

//get SnapFiles in SnapDir
// *.log,*.idx,*.tmp
func (s *SnapReceiveAgent) GetSnapFilesFromDir() ([]*SnapFile, error) {
	existFiles, err := ioutil.ReadDir(s.SnapDir)
	if err != nil {
		glog.Errorf("read dir error in GetExistSnapFiles,err=%s,raftGroupSnapDir=%s", err.Error(), s.SnapDir)
		return nil, err
	}

	snapFiles := make([]*SnapFile, 0, 10)
	for _, file := range existFiles {
		fileName := file.Name()
		if strings.HasSuffix(fileName, SegmentFileSuffix) {
			segmentFile := &SnapFile{
				FileName: fileName,
				FileType: SegmentFileType,
			}
			snapFiles = append(snapFiles, segmentFile)
		} else if strings.HasSuffix(fileName, IndexFileSuffix) {
			indexFile := &SnapFile{
				FileName: fileName,
				FileType: IndexFileType,
			}
			snapFiles = append(snapFiles, indexFile)
		} else if strings.HasSuffix(fileName, TmpFileSuffix) {
			tmpFile := &SnapFile{
				FileName: fileName,
				FileType: TmpFileType,
			}
			snapFiles = append(snapFiles, tmpFile)
		}
	}

	return snapFiles, nil
}

//从raft_group_snapshot_meta.json中获得SnapFile
func (s *SnapReceiveAgent) GetSnapFilesFromMeta() ([]*SnapFile, error) {
	if s.SnapMeta == nil || len(s.SnapMeta.Segments) == 0 {
		return nil, errSnapMetaNil
	}

	snapFiles := make([]*SnapFile, 0, len(s.SnapMeta.Segments)*2)
	for i, segment := range s.SnapMeta.Segments {
		isLast := (i == len(s.SnapMeta.Segments)-1)
		segmentFile := &SnapFile{
			FileName: segment.SegmentName,
			FileType: SegmentFileType,
			IsLast:   isLast,
		}
		indexFile := &SnapFile{
			FileName: segment.Index.IndexName,
			FileType: IndexFileType,
			IsLast:   isLast,
		}
		snapFiles = append(snapFiles, segmentFile, indexFile)
	}
	return snapFiles, nil
}

func (s *SnapReceiveAgent) GetSegmentFile(segmentName string, isLastSegment bool) error {
	baseUrl := "http://" + s.ServerAddr + "/api/get_segment"
	req, err := http.NewRequest("GET", baseUrl, nil)
	if err != nil {
		return err
	}
	//add args
	q := req.URL.Query()
	q.Add("raft_group", s.RaftGroup)
	q.Add("is_last_segment", strconv.FormatBool(isLastSegment))
	q.Add("segment_name", segmentName)
	req.URL.RawQuery = q.Encode()
	normalUrl := req.URL.String()

	filePath, err := s.downloadFile(normalUrl)
	if err != nil {
		glog.Errorf("downloadFile in GetSegmentFile error,err=%s,normalUrl=%s", err.Error(), normalUrl)
		return err
	}
	glog.Infof("Download segment file:%s success", filePath)
	return nil
}

func (s *SnapReceiveAgent) GetIndexFile(indexName string, isLastIndex bool) error {
	baseUrl := "http://" + s.ServerAddr + "/api/get_index"
	req, err := http.NewRequest("GET", baseUrl, nil)
	if err != nil {
		return err
	}

	//add args
	q := req.URL.Query()
	q.Add("raft_group", s.RaftGroup)
	q.Add("is_last_index", strconv.FormatBool(isLastIndex))
	q.Add("index_name", indexName)
	req.URL.RawQuery = q.Encode()
	normalUrl := req.URL.String()

	filePath, err := s.downloadFile(normalUrl)
	if err != nil {
		glog.Errorf("downloadFile in GetIndexFile error,err=%s,normalUrl=%s", err.Error(), normalUrl)
		return err
	}
	glog.Infof("Download index file:%s success", filePath)
	return nil
}

func (s *SnapReceiveAgent) GetSnapMeta() (*apiserver.SnapshotMeta, error) {
	var snapMeta apiserver.SnapshotMeta

	baseUrl := "http://" + s.ServerAddr + "/api/get_snap"
	req, err := http.NewRequest("GET", baseUrl, nil)
	if err != nil {
		return nil, err
	}

	//add args
	q := req.URL.Query()
	q.Add("raft_group", s.RaftGroup)
	req.URL.RawQuery = q.Encode()
	normalUrl := req.URL.String()

	filePath, err := s.downloadFile(normalUrl)
	if err != nil {
		glog.Errorf("downloadFile in GetSnapMeta error,err=%s,normalUrl=%s", err.Error(), normalUrl)
		return nil, err
	}
	glog.Infof("Download MetaFile:%s success", filePath)

	isFileExist, err := pathExists(filePath)
	if err != nil {
		return nil, err
	}

	if isFileExist {
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &snapMeta); err != nil {
			return nil, err
		}
	} else {
		glog.Errorf("metadata file not exist,filePath=%s", filePath)
		return nil, errFileNotExist
	}

	//check metadata of snapshot
	if snapMeta.ApplyIndex == 0 || snapMeta.Vote.Term == 0 ||
		len(snapMeta.Members.MembersMap) == 0 ||
		snapMeta.RaftGroupName != s.RaftGroup {
		glog.Fatalf("The metadata of snapshot error,snapMeta=%v,s.raftGroup=%s", snapMeta, s.RaftGroup)
	}

	return &snapMeta, nil
}

func (s *SnapReceiveAgent) DownloadSnapFiles(files []*SnapFile) error {
	var err error

	//log the file list
	glog.Infof("start download files,file list is:")
	for i, file := range files {
		glog.Infof("i=%d,fileName=%s\n", i, file.FileName)
	}

	for i := 0; i < len(files); i++ {
		if files[i].FileType == SegmentFileType {
			err = s.GetSegmentFile(files[i].FileName, files[i].IsLast)
			if err != nil {
				glog.Errorf("GetSegmentFile error,err=%s,fileName=%s,isLast=%v", err.Error(), files[i].FileName, files[i].IsLast)
				return err
			}
			glog.Infof("GetSegmentFile success,fileName=%s,isLast=%v,download_progress=[%d/%d]",
				files[i].FileName, files[i].IsLast, i+1, len(files))
		} else if files[i].FileType == IndexFileType {
			err = s.GetIndexFile(files[i].FileName, files[i].IsLast)
			if err != nil {
				glog.Errorf("GetIndexFile error,err=%s,fileName=%s,isLast=%v", err.Error(), files[i].FileName, files[i].IsLast)
				return err
			}
			glog.Infof("GetIndexFile success,fileName=%s,isLast=%v,download_progress=[%d/%d]",
				files[i].FileName, files[i].IsLast, i+1, len(files))
		}
	}
	return err
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

func (s *SnapReceiveAgent) downloadFile(url string) (string, error) {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		glog.Errorf("http.Get error,err=%s,url=%s", err.Error(), url)
		return "", err
	}
	defer resp.Body.Close()

	fileInfo, err := s.getFileInfoFromHeader(resp.Header)
	if err != nil {
		glog.Errorf("getFileInfoFromHeader error,err=%s,header=%v\n", err.Error(), resp.Header)
		return "", err
	}

	tmpFilePath, err := s.saveFile(resp.Body, fileInfo)
	if err != nil {
		glog.Errorf("saveFile error,err=%s,fileInfo=%v\n", err.Error(), *fileInfo)
		return "", err
	}

	filePath := path.Join(s.SnapDir, fileInfo.FileName)
	err = os.Rename(tmpFilePath, filePath)
	if err != nil {
		return "", err
	}
	return filePath, nil
}

func (s *SnapReceiveAgent) createFileWithSize(filePath string, fileSize int) (*os.File, error) {
	if len(filePath) == 0 || fileSize < 0 {
		return nil, errArgsNotAvail
	}

	//filePath := path.Join(s.SnapDir, fileName)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, FilePermission)
	if err != nil {
		glog.Errorf("createFileWithSize error,err=%s,path=%s", err.Error(), filePath)
		return nil, err
	}

	//pre allocate maxSize space for this file
	if err = fileutil.Preallocate(file, int64(fileSize), true); err != nil {
		glog.Errorf("failed to allocate space when creating new file (%v)", err)
		return nil, err
	}
	return file, nil
}

func (s *SnapReceiveAgent) getFileInfoFromHeader(header http.Header) (*FileInfo, error) {
	//get file name
	contentDis := header.Get("Content-Disposition")
	_, params, err := mime.ParseMediaType(contentDis)
	if err != nil {
		glog.Errorf("mime.ParseMediaType error,err=%s,header=%v,contentDis=%s", err.Error(), header, contentDis)
		return nil, err
	}
	fileName := params["filename"]
	if len(fileName) == 0 {
		glog.Errorf("fileName is nil,header=%s", header)
		return nil, errHttp
	}
	//get file checksum
	md5str := header.Get("File-Md5")
	if len(md5str) == 0 {
		glog.Errorf("md5str is nil,header=%s", header)
		return nil, errHttp
	}
	//get file size
	sizeStr := header.Get("Content-Length")
	if len(sizeStr) == 0 {
		glog.Errorf("sizeStr is nil,header=%s", header)
		return nil, errHttp
	}
	fileSize, err := strconv.Atoi(sizeStr)
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		FileName: fileName,
		FileSize: fileSize,
		CheckSum: md5str,
	}, nil
}

func (s *SnapReceiveAgent) saveFile(body io.ReadCloser, fileInfo *FileInfo) (string, error) {
	tmpFilePath := path.Join(s.SnapDir, fileInfo.FileName+TmpFileSuffix)
	tmpFile, err := s.createFileWithSize(tmpFilePath, fileInfo.FileSize)
	if err != nil {
		glog.Errorf("createFileWithSize error,err=%s,filePath=%s,fileSize=%d",
			err.Error(), tmpFilePath, fileInfo.FileSize)
		return "", err
	}
	defer tmpFile.Close()

	counter := &WriteCounter{
		FileName:   fileInfo.FileName + TmpFileSuffix,
		Total:      0,
		PrintTimes: 0,
	}
	_, err = io.Copy(tmpFile, io.TeeReader(body, counter))
	if err != nil {
		glog.Errorf("io.Copy error,err=%s,filePath=%s,fileSize=%d", err.Error(), tmpFilePath, fileInfo.FileSize)
		return "", err
	}
	glog.Infof("Download file complete,total size is:%s... %.2f MB\n",
		counter.FileName, float64(counter.Total)/(1024*1024))

	err = s.checkSum(tmpFile, fileInfo.CheckSum)
	if err != nil {
		glog.Errorf("checkSum error,err=%s,filePath=%s,fileSize=%d,md5str=%s",
			err.Error(), tmpFilePath, fileInfo.FileSize, fileInfo.CheckSum)
		return "", err
	}

	return tmpFilePath, nil
}

func (s *SnapReceiveAgent) checkSum(file *os.File, md5str string) error {
	if file == nil || len(md5str) == 0 {
		return errArgsNotAvail
	}

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		glog.Errorf("Seek error,err=%s,fileName=%s", err.Error(), file.Name())
		return err
	}

	md5Value := md5.New()
	io.Copy(md5Value, file)
	newMd5str := hex.EncodeToString(md5Value.Sum(nil))
	if newMd5str != md5str {
		glog.Errorf("checkSum md5 not match,fileName=%s,md5str=%v,newMd5str=%v",
			file.Name(), md5str, newMd5str)
		return errMd5NotMatch
	}
	return nil
}

func inSlice(f *SnapFile, sl []*SnapFile) bool {
	for _, vv := range sl {
		if vv.FileName == f.FileName {
			return true
		}
	}
	return false
}

// sliceDiff returns diff slice of slice1 - slice2
func sliceDiff(slice1, slice2 []*SnapFile) []*SnapFile {
	diffslice := make([]*SnapFile, 0, 10)
	for _, v := range slice1 {
		if !inSlice(v, slice2) {
			diffslice = append(diffslice, v)
		}
	}
	return diffslice
}

// sliceIntersect returns slice that are present in all the slice1 and slice2.
func sliceIntersect(slice1, slice2 []*SnapFile) []*SnapFile {
	allInslice := make([]*SnapFile, 0, 10)
	for _, v := range slice1 {
		if inSlice(v, slice2) {
			allInslice = append(allInslice, v)
		}
	}
	return allInslice
}

type WriteCounter struct {
	FileName   string
	Total      uint64
	PrintTimes uint64
}

func (wc *WriteCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.Total += uint64(n)
	wc.PrintProgress()
	return n, nil
}

func (wc *WriteCounter) PrintProgress() {
	//每传输10MB打印一次Log
	if wc.PrintTimes*1024*1024*10 <= wc.Total {
		glog.Infof("Downloading file:%s... %.2f MB complete\n",
			wc.FileName, float64(wc.Total)/(1024*1024))
		wc.PrintTimes++
	}
}

func writeDownloadStatus(filePath string, status string) error {
	err := ioutil.WriteFile(filePath, []byte(status), FilePermission)
	if err != nil {
		return err
	}
	return nil
}
