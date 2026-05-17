// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

func applyInstallPreparedMVCCEntriesBatch(db txnstore.Store, reqs []*kvrpcpb.InstallPreparedMVCCEntriesRequest) ([]*kvrpcpb.InstallPreparedMVCCEntriesResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	responses := make([]*kvrpcpb.InstallPreparedMVCCEntriesResponse, len(reqs))
	var entries []*entrykv.Entry
	offsets := make([]int, len(reqs)+1)
	for idx, req := range reqs {
		batch, keyErr := buildInstallPreparedMVCCEntries(req)
		if keyErr != nil {
			releaseEntries(entries)
			return installPreparedMVCCErrorResponses(len(reqs), keyErr), nil
		}
		offsets[idx] = len(entries)
		entries = append(entries, batch...)
		responses[idx] = &kvrpcpb.InstallPreparedMVCCEntriesResponse{
			AppliedEntries: uint64(len(batch)),
			CommitVersion:  req.GetCommitVersion(),
		}
	}
	offsets[len(reqs)] = len(entries)
	defer releaseEntries(entries)
	if len(entries) == 0 {
		return responses, nil
	}
	if err := db.ApplyInternalEntries(entries); err != nil {
		return nil, err
	}
	for idx := range responses {
		responses[idx].AppliedEntries = uint64(offsets[idx+1] - offsets[idx])
	}
	return responses, nil
}

func installPreparedMVCCErrorResponses(count int, keyErr *kvrpcpb.KeyError) []*kvrpcpb.InstallPreparedMVCCEntriesResponse {
	responses := make([]*kvrpcpb.InstallPreparedMVCCEntriesResponse, count)
	for i := range responses {
		responses[i] = &kvrpcpb.InstallPreparedMVCCEntriesResponse{Error: keyErr}
	}
	return responses
}

func buildInstallPreparedMVCCEntries(req *kvrpcpb.InstallPreparedMVCCEntriesRequest) ([]*entrykv.Entry, *kvrpcpb.KeyError) {
	if req == nil {
		return nil, installPreparedMVCCAbort("missing prepared mvcc install request")
	}
	if len(req.GetRoutingKey()) == 0 {
		return nil, installPreparedMVCCAbort("missing routing key")
	}
	if req.GetCommitVersion() == 0 || req.GetCommitVersion() == entrykv.MaxVersion {
		return nil, installPreparedMVCCAbort("invalid commit version")
	}
	if len(req.GetEntries()) == 0 {
		return nil, nil
	}
	entries := make([]*entrykv.Entry, 0, len(req.GetEntries()))
	for _, prepared := range req.GetEntries() {
		entry, keyErr := preparedMVCCEntryToInternal(req.GetCommitVersion(), prepared)
		if keyErr != nil {
			releaseEntries(entries)
			return nil, keyErr
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func preparedMVCCEntryToInternal(commitVersion uint64, prepared *kvrpcpb.PreparedMVCCEntry) (*entrykv.Entry, *kvrpcpb.KeyError) {
	if prepared == nil {
		return nil, installPreparedMVCCAbort("nil prepared mvcc entry")
	}
	cf, ok := preparedMVCCColumnFamily(prepared.GetColumnFamily())
	if !ok {
		return nil, installPreparedMVCCAbort("invalid column family")
	}
	if len(prepared.GetKey()) == 0 {
		return nil, installPreparedMVCCAbort("empty key")
	}
	if prepared.GetVersion() != commitVersion {
		return nil, installPreparedMVCCAbort("entry version does not match commit version")
	}
	if prepared.GetMeta() > 0xff {
		return nil, installPreparedMVCCAbort("entry meta out of range")
	}
	var value []byte
	if prepared.GetHasValue() {
		value = clonePreparedMVCCValue(prepared.GetValue())
	}
	return entrykv.NewInternalEntry(cf, prepared.GetKey(), prepared.GetVersion(), value, byte(prepared.GetMeta()), prepared.GetExpiresAt()), nil
}

func preparedMVCCColumnFamily(cf kvrpcpb.PreparedMVCCEntry_ColumnFamily) (entrykv.ColumnFamily, bool) {
	switch cf {
	case kvrpcpb.PreparedMVCCEntry_DEFAULT:
		return entrykv.CFDefault, true
	case kvrpcpb.PreparedMVCCEntry_LOCK:
		return entrykv.CFLock, true
	case kvrpcpb.PreparedMVCCEntry_WRITE:
		return entrykv.CFWrite, true
	default:
		return entrykv.CFDefault, false
	}
}

func clonePreparedMVCCValue(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return append([]byte(nil), value...)
}

func installPreparedMVCCAbort(msg string) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Abort: msg}
}
