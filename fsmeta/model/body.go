// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"bytes"
	"encoding/json"
	"io"
)

// BodyDescriptor is the compact object/body manifest stored in inode
// OpaqueAttrs for artifact-style files. fsmeta does not own the external body;
// it only publishes this descriptor atomically with the namespace entry.
type BodyDescriptor struct {
	Producer    string `json:"producer"`
	DigestURI   string `json:"digest_uri"`
	Size        uint64 `json:"size"`
	ContentType string `json:"content_type"`
	BodyRef     string `json:"body_ref"`
	Generation  uint64 `json:"generation"`
}

// EncodeBodyDescriptor encodes desc into the canonical OpaqueAttrs JSON form.
func EncodeBodyDescriptor(desc BodyDescriptor) ([]byte, error) {
	if err := ValidateBodyDescriptor(desc); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(desc)
	if err != nil {
		return nil, ErrInvalidValue
	}
	if len(encoded) > MaxInodeOpaqueAttrsBytes {
		return nil, ErrInvalidValue
	}
	return encoded, nil
}

// DecodeBodyDescriptor decodes an OpaqueAttrs body descriptor.
func DecodeBodyDescriptor(data []byte) (BodyDescriptor, error) {
	if len(data) == 0 {
		return BodyDescriptor{}, ErrInvalidValue
	}
	var desc BodyDescriptor
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&desc); err != nil {
		return BodyDescriptor{}, ErrInvalidValue
	}
	var extra struct{}
	if err := dec.Decode(&extra); err != io.EOF {
		return BodyDescriptor{}, ErrInvalidValue
	}
	if err := ValidateBodyDescriptor(desc); err != nil {
		return BodyDescriptor{}, err
	}
	return desc, nil
}

// InodeBodyDescriptor decodes inode.OpaqueAttrs when it contains a body
// descriptor. Empty OpaqueAttrs reports ok=false; malformed non-empty attrs are
// surfaced because callers use this path for GC decisions.
func InodeBodyDescriptor(inode InodeRecord) (BodyDescriptor, bool, error) {
	if len(inode.OpaqueAttrs) == 0 {
		return BodyDescriptor{}, false, nil
	}
	desc, err := DecodeBodyDescriptor(inode.OpaqueAttrs)
	if err != nil {
		return BodyDescriptor{}, false, err
	}
	return desc, true, nil
}

// ValidateBodyDescriptor validates the manifest shape fsmeta stores in
// OpaqueAttrs. BodyRef is the stable external reference required for cleanup
// and retry; other fields are metadata annotations.
func ValidateBodyDescriptor(desc BodyDescriptor) error {
	if desc.BodyRef == "" {
		return ErrInvalidValue
	}
	if containsNUL(desc.Producer) ||
		containsNUL(desc.DigestURI) ||
		containsNUL(desc.ContentType) ||
		containsNUL(desc.BodyRef) {
		return ErrInvalidValue
	}
	return nil
}

func containsNUL(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return true
		}
	}
	return false
}
