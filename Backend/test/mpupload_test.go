package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	baseURL = "http://localhost:8081"
)

func TestMultipartUpload(t *testing.T) {
	// Step 1: Initialize multipart upload
	initResp, err := http.Post(baseURL+"/file/mpupload/init", "application/json", bytes.NewBuffer([]byte(`{"filehash":"testhash","filename":"testfile","filesize":1024}`)))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, initResp.StatusCode)

	var initResult map[string]interface{}
	err = json.NewDecoder(initResp.Body).Decode(&initResult)
	assert.NoError(t, err)
	uploadID := initResult["UploadID"].(string)

	// Step 2: Upload parts in parallel
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(partNumber int) {
			defer wg.Done()
			partData := []byte("part data")
			partResp, err := http.Post(baseURL+"/file/mpupload/uploadpart", "application/octet-stream", bytes.NewBuffer(partData))
			assert.NoError(t, err)
			assert.Equal(t, http.StatusOK, partResp.StatusCode)
		}(i)
	}
	wg.Wait()

	// Step 3: Complete multipart upload
	completeResp, err := http.Post(baseURL+"/file/mpupload/complete", "application/json", bytes.NewBuffer([]byte(`{"uploadid":"`+uploadID+`"}`)))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, completeResp.StatusCode)

	var completeResult map[string]interface{}
	err = json.NewDecoder(completeResp.Body).Decode(&completeResult)
	assert.NoError(t, err)
	assert.Equal(t, "ok", completeResult["status"])

	// Clean up - Optional: depending on how your API is designed, this might involve deleting keys from Redis, etc.
}
