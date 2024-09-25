package api

import (
	"bytes"
	"cloud_distributed_storage/Backend/common"
	cfg "cloud_distributed_storage/Backend/config"
	"cloud_distributed_storage/Backend/mq"
	dbcli "cloud_distributed_storage/Backend/service/dbproxy/client"
	"cloud_distributed_storage/Backend/service/dbproxy/orm"
	"cloud_distributed_storage/Backend/store/minio"
	"cloud_distributed_storage/Backend/store/s3"
	"cloud_distributed_storage/Backend/util"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
)

// UploadHandler: handle file upload
func UploadHandler(c *gin.Context) {
	errCode := 0
	defer func() {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		if errCode < 0 {
			c.JSON(http.StatusOK, gin.H{
				"code": errCode,
				"msg":  "上传失败",
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"code": errCode,
				"msg":  "上传成功",
			})
		}
	}()
	// parse request
	file, head, err := c.Request.FormFile("file")
	if err != nil {
		log.Printf("Failed to get form data, err:%s\n", err.Error())
		errCode = -1
		return
	}
	username, exists := c.Get("username")
	if !exists {
		log.Printf("Failed to get file data, err:%s\n", err.Error())
		errCode = -2
		return
	}
	defer file.Close()

	// 2. 把文件内容转为[]byte
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, file); err != nil {
		log.Printf("Failed to get file data, err:%s\n", err.Error())
		errCode = -2
		return
	}

	// 3. 构建文件元信息
	fileMeta := dbcli.FileMeta{
		FileName: head.Filename,
		FileSha1: util.Sha1(buf.Bytes()), //　计算文件sha1
		FileSize: int64(len(buf.Bytes())),
		UploadAt: time.Now().Format("2006-01-02 15:04:05"),
	}

	// 判断存储策略
	var storageType string
	if isImportantFile(fileMeta) {
		storageType = "ceph"
	} else {
		storageType = "s3"
	}

	// 4. 将文件写入临时存储位置
	fileMeta.Location = cfg.TempLocalRootDir + fileMeta.FileSha1 // 临时存储地址
	newFile, err := os.Create(fileMeta.Location)
	if err != nil {
		log.Printf("Failed to create file, err:%s\n", err.Error())
		errCode = -3
		return
	}
	defer newFile.Close()

	nByte, err := newFile.Write(buf.Bytes())
	if int64(nByte) != fileMeta.FileSize || err != nil {
		log.Printf("Failed to save data into file, writtenSize:%d, err:%s\n", nByte, err.Error())
		errCode = -4
		return
	}
	// 5. 同步或异步将文件转移到Ceph/S3
	newFile.Seek(0, 0) // 游标重新回到文件头部

	switch storageType {
	case "ceph":
		// save file into ceph

		data, _ := ioutil.ReadAll(newFile)
		cephPath := cfg.MinioRootDir + fileMeta.FileSha1
		_ = minio.PutObject("filestore", cephPath, data, "application/octet-stream")
		fileMeta.Location = cephPath
		if err != nil {
			c.JSON(http.StatusOK, util.NewRespMsg(-1, "Failed to save data into Ceph", nil))
			return
		}
		fileMeta.Location = cephPath
	case "s3":
		// 文件写入S3存储
		s3Path := cfg.S3RootDir + fileMeta.FileSha1
		// 判断写入S3为同步还是异步
		if !cfg.AsyncTransferEnable {
			// TODO: 设置s3中的文件名，方便指定文件名下载
			s3Client := s3.GetS3Client()
			bucketBasics := s3.BucketBasics{S3Client: s3Client}
			err = bucketBasics.UploadFile(cfg.S3_BUCKET_NAME, s3Path, newFile)
			if err != nil {
				log.Println(err.Error())
				errCode = -5
				return
			}
			fileMeta.Location = s3Path
		} else {
			data := mq.TransferData{
				FileHash:      fileMeta.FileSha1,
				CurLocation:   fileMeta.Location,
				DestLocation:  s3Path,
				DestStoreType: common.StoreS3,
			}
			pubData, _ := json.Marshal(data)
			log.Printf("start to publish message to transcode: %s\n", pubData)
			pubSuc := mq.Publish(cfg.TransExchangeName, cfg.TransS3RoutingKey, pubData)
			if !pubSuc {
				log.Println("文件转移消息发送失败，稍后重试")
			}
		}
	}

	//6.  更新文件表记录
	_, err = dbcli.OnFileUploadFinished(fileMeta)
	if err != nil {
		errCode = -6
		return
	}

	// 更新用户文件表记录
	upRes, err := dbcli.OnUserFileUploadFinished(username.(string), fileMeta)
	if err == nil && upRes.Suc {
		errCode = 0
	} else {
		errCode = -6
	}
}

// TryFastUploadHandler : 尝试秒传接口
func TryFastUploadHandler(c *gin.Context) {

	// 1. 解析请求参数
	username := c.Request.FormValue("username")
	filehash := c.Request.FormValue("filehash")
	filename := c.Request.FormValue("filename")
	// filesize, _ := strconv.Atoi(c.Request.FormValue("filesize"))

	// 2. 从文件表中查询相同hash的文件记录
	fileMetaResp, err := dbcli.GetFileMeta(filehash)
	if err != nil {
		log.Println(err.Error())
		c.Status(http.StatusInternalServerError)
		return
	}

	// 3. 查不到记录则返回秒传失败
	if !fileMetaResp.Suc {
		resp := util.RespMsg{
			Code: -1,
			Msg:  "秒传失败，请访问普通上传接口",
		}
		c.Data(http.StatusOK, "application/json", resp.JSONBytes())
		return
	}

	// 4. 上传过则将文件信息写入用户文件表， 返回成功
	fmeta := dbcli.TableFileToFileMeta(fileMetaResp.Data.(orm.TableFile))
	fmeta.FileName = filename
	upRes, err := dbcli.OnUserFileUploadFinished(username, fmeta)
	if err == nil && upRes.Suc {
		resp := util.RespMsg{
			Code: 0,
			Msg:  "秒传成功",
		}
		c.Data(http.StatusOK, "application/json", resp.JSONBytes())
		return
	}
	resp := util.RespMsg{
		Code: -2,
		Msg:  "秒传失败，请稍后重试",
	}
	c.Data(http.StatusOK, "application/json", resp.JSONBytes())
	return
}

// 判断文件是否为重要文件
func isImportantFile(fileMeta dbcli.FileMeta) bool {
	// 可以根据文件名、大小、类型等条件判断
	matched, _ := regexp.MatchString("VI$", fileMeta.FileName)
	if matched {
		return true
	}
	return false
}
