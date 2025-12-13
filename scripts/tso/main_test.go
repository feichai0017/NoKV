package main

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseBatchParam(t *testing.T) {
	req := httptest.NewRequest("GET", "/tso?batch=5", nil)
	b, err := parseBatchParam(req)
	require.NoError(t, err)
	require.Equal(t, uint64(5), b)

	req = httptest.NewRequest("GET", "/tso?batch=not-a-number", nil)
	_, err = parseBatchParam(req)
	require.Error(t, err)

	req = httptest.NewRequest("GET", "/tso?batch=0", nil)
	_, err = parseBatchParam(req)
	require.Error(t, err)

	b, err = parseBatchParam(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), b)
}

func TestHandleTSOAllocatesSequentialTimestamps(t *testing.T) {
	atomic.StoreUint64(&counter, 0)

	// First request with batch=3.
	req := httptest.NewRequest("GET", "/tso", nil)
	q := url.Values{}
	q.Set("batch", "3")
	req.URL.RawQuery = q.Encode()
	rec := httptest.NewRecorder()
	handleTSO(rec, req)

	require.Equal(t, 200, rec.Code)
	var resp tsoResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, uint64(1), resp.Timestamp)
	require.Equal(t, uint64(3), resp.Count)

	// Second request should continue the sequence.
	req2 := httptest.NewRequest("GET", "/tso?batch=2", nil)
	rec2 := httptest.NewRecorder()
	handleTSO(rec2, req2)

	require.Equal(t, 200, rec2.Code)
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Equal(t, uint64(4), resp.Timestamp) // previous latest was 3
	require.Equal(t, uint64(2), resp.Count)
}
