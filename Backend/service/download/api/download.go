package api

import (
	"cloud_distributed_storage/Backend/common"
	cfg "cloud_distributed_storage/Backend/config"
	dbcli "cloud_distributed_storage/Backend/service/dbproxy/client"
	minio "cloud_distributed_storage/Backend/store/minio"
	s3Client "cloud_distributed_storage/Backend/store/s3"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

func GeneratePresignedURL(bucketName, objectKey string, expiry time.Duration) (string, error) {
	s3Client := s3Client.GetS3Client()
	presignClient := s3.NewPresignClient(s3Client)
	presignResult, err := presignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		log.Printf("Couldn't get presigned URL for object %v:%v. Here's why: %v\n", bucketName, objectKey, err)
		return "", err
	}
	return presignResult.URL, nil
}

// DownloadURLHandler : 生成文件的下载地址
func DownloadURLHandler(c *gin.Context) {
	filehash := c.Request.FormValue("filehash")
	// 从文件表查找记录
	dbResp, err := dbcli.GetFileMeta(filehash)
	if err != nil {
		c.JSON(
			http.StatusOK,
			gin.H{
				"code": common.StatusServerError,
				"msg":  "server error",
			})
		return
	}

	tblFile := dbcli.ToTableFile(dbResp.Data)

	// TODO: 判断文件存在S3，还是Ceph，还是在本地
	if strings.HasPrefix(tblFile.FileAddr.String, cfg.TempLocalRootDir) ||
		strings.HasPrefix(tblFile.FileAddr.String, cfg.MinioRootDir) {
		username := c.Request.FormValue("username")
		token := c.Request.FormValue("token")
		tmpURL := fmt.Sprintf("http://%s/file/download?filehash=%s&username=%s&token=%s",
			c.Request.Host, filehash, username, token)
		c.Data(http.StatusOK, "application/octet-stream", []byte(tmpURL))
	} else if strings.HasPrefix(tblFile.FileAddr.String, cfg.S3RootDir) {
		// s3下载url
		s3path := cfg.S3RootDir + filehash
		signedURL, err := GeneratePresignedURL(cfg.S3_BUCKET_NAME, s3path, 15*time.Minute)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"code": common.StatusServerError,
				"msg":  "server error",
			})
			return
		}
		c.Data(http.StatusOK, "application/octet-stream", []byte(signedURL))
	}
}

// DownloadHandler : 文件下载接口
func DownloadHandler(c *gin.Context) {
	fsha1 := c.Request.FormValue("filehash")
	username := c.Request.FormValue("username")
	// TODO: 处理异常情况
	fResp, ferr := dbcli.GetFileMeta(fsha1)
	ufResp, uferr := dbcli.QueryUserFileMeta(username, fsha1)
	if ferr != nil || uferr != nil || !fResp.Suc || !ufResp.Suc {
		c.JSON(
			http.StatusOK,
			gin.H{
				"code": common.StatusServerError,
				"msg":  "server error",
			})
		return
	}
	uniqFile := dbcli.ToTableFile(fResp.Data)
	userFile := dbcli.ToTableUserFile(ufResp.Data)

	if strings.HasPrefix(uniqFile.FileAddr.String, cfg.TempLocalRootDir) {
		// 本地文件， 直接下载
		c.FileAttachment(uniqFile.FileAddr.String, userFile.FileName)
	} else if strings.HasPrefix(uniqFile.FileAddr.String, cfg.MinioRootDir) {
		// minio中的文件，通过minio api先下载
		data, _ := minio.GetObject("filestore", uniqFile.FileAddr.String)
		//	c.Header("content-type", "application/octect-stream")
		c.Header("content-disposition", "attachment; filename=\""+userFile.FileName+"\"")
		c.Data(http.StatusOK, "application/octect-stream", data)
	}
}
