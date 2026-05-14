// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
)

const defaultPageLimit uint32 = 256

type scrubClient interface {
	ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
	Close() error
}

type scrubIssueKind string

const (
	issueInvalidDentryName    scrubIssueKind = "invalid_dentry_name"
	issueDentryParentMismatch scrubIssueKind = "dentry_parent_mismatch"
	issueDentryInodeMismatch  scrubIssueKind = "dentry_inode_mismatch"
	issueDentryTypeMismatch   scrubIssueKind = "dentry_type_mismatch"
	issueDirectoryHardlink    scrubIssueKind = "directory_hardlink"
	issueLinkCountMismatch    scrubIssueKind = "link_count_mismatch"
	issuePaginationRegression scrubIssueKind = "pagination_regression"
	issueIssueLimitExhausted  scrubIssueKind = "issue_limit_exhausted"
)

type scrubIssue struct {
	Kind   scrubIssueKind `json:"kind"`
	Parent fsmeta.InodeID `json:"parent,omitempty"`
	Name   string         `json:"name,omitempty"`
	Inode  fsmeta.InodeID `json:"inode,omitempty"`
	Detail string         `json:"detail,omitempty"`
}

type scrubReport struct {
	Mount       fsmeta.MountID `json:"mount"`
	Root        fsmeta.InodeID `json:"root"`
	Directories uint64         `json:"directories"`
	Dentries    uint64         `json:"dentries"`
	Inodes      uint64         `json:"inodes"`
	Issues      []scrubIssue   `json:"issues,omitempty"`
}

func (r scrubReport) OK() bool {
	return len(r.Issues) == 0
}

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC address")
		mount     = flag.String("mount", "default", "registered mount ID")
		root      = flag.Uint64("root", uint64(fsmeta.RootInode), "root inode to scrub")
		pageLimit = flag.Uint("page-limit", uint(defaultPageLimit), "ReadDirPlus page size")
		maxIssues = flag.Int("max-issues", 32, "maximum issues to record before failing fast")
		timeout   = flag.Duration("timeout", 60*time.Second, "overall scrub timeout")
		jsonOut   = flag.Bool("json", false, "emit JSON report")
	)
	flag.Parse()
	if *addr == "" || *mount == "" || *root == 0 || *pageLimit == 0 || *pageLimit > uint(fsmeta.MaxReadDirLimit) || *timeout <= 0 {
		log.Fatalf("addr, mount, root, page-limit, and timeout must be valid")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(ctx, *addr)
	if err != nil {
		log.Fatalf("dial fsmeta %s: %v", *addr, err)
	}
	defer func() { _ = cli.Close() }()

	report, err := scrubMount(ctx, cli, fsmeta.MountID(*mount), fsmeta.InodeID(*root), uint32(*pageLimit), *maxIssues)
	if err != nil {
		log.Fatalf("scrub failed: %v", err)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			log.Fatalf("encode report: %v", err)
		}
	} else {
		fmt.Printf("fsmeta scrub mount=%s root=%d directories=%d dentries=%d inodes=%d issues=%d\n",
			report.Mount, report.Root, report.Directories, report.Dentries, report.Inodes, len(report.Issues))
		for _, issue := range report.Issues {
			fmt.Printf("issue kind=%s parent=%d name=%q inode=%d detail=%s\n",
				issue.Kind, issue.Parent, issue.Name, issue.Inode, issue.Detail)
		}
	}
	if !report.OK() {
		os.Exit(1)
	}
}

func scrubMount(ctx context.Context, cli scrubClient, mount fsmeta.MountID, root fsmeta.InodeID, pageLimit uint32, maxIssues int) (scrubReport, error) {
	if cli == nil {
		return scrubReport{}, fmt.Errorf("scrub client is required")
	}
	if mount == "" || root == 0 {
		return scrubReport{}, fsmeta.ErrInvalidRequest
	}
	if pageLimit == 0 {
		pageLimit = defaultPageLimit
	}
	report := scrubReport{Mount: mount, Root: root}
	refs := make(map[fsmeta.InodeID]uint32)
	inodes := make(map[fsmeta.InodeID]fsmeta.InodeRecord)
	visitedDirs := make(map[fsmeta.InodeID]struct{})
	queue := []fsmeta.InodeID{root}

	addIssue := func(issue scrubIssue) bool {
		if maxIssues > 0 && len(report.Issues) >= maxIssues {
			report.Issues = append(report.Issues, scrubIssue{Kind: issueIssueLimitExhausted, Detail: "issue limit reached"})
			return false
		}
		report.Issues = append(report.Issues, issue)
		return true
	}

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		parent := queue[0]
		queue = queue[1:]
		if _, ok := visitedDirs[parent]; ok {
			continue
		}
		visitedDirs[parent] = struct{}{}
		report.Directories++
		entries, err := readDirPlusAll(ctx, cli, fsmeta.ReadDirRequest{
			Mount:  mount,
			Parent: parent,
			Limit:  pageLimit,
		})
		if err != nil {
			return report, err
		}
		for _, pair := range entries {
			if !scrubDentryPair(pair, parent, refs, inodes, &queue, addIssue) {
				return report, nil
			}
			report.Dentries++
		}
	}

	report.Inodes = uint64(len(inodes))
	for inodeID, refCount := range refs {
		inode := inodes[inodeID]
		if inode.LinkCount != refCount {
			if !addIssue(scrubIssue{
				Kind:   issueLinkCountMismatch,
				Inode:  inodeID,
				Detail: fmt.Sprintf("link_count=%d refs=%d", inode.LinkCount, refCount),
			}) {
				return report, nil
			}
		}
	}
	sortScrubIssues(report.Issues)
	return report, nil
}

func readDirPlusAll(ctx context.Context, cli scrubClient, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	limit := req.Limit
	if limit == 0 {
		limit = defaultPageLimit
	}
	var out []fsmeta.DentryAttrPair
	lastName := ""
	haveLast := false
	for {
		req.Limit = limit
		page, err := cli.ReadDirPlus(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		for _, pair := range page {
			name := pair.Dentry.Name
			if haveLast && name <= lastName {
				return out, fmt.Errorf("%s: parent=%d previous=%q current=%q", issuePaginationRegression, req.Parent, lastName, name)
			}
			lastName = name
			haveLast = true
		}
		out = append(out, page...)
		if uint32(len(page)) < limit {
			return out, nil
		}
		req.StartAfter = page[len(page)-1].Dentry.Name
	}
}

func scrubDentryPair(
	pair fsmeta.DentryAttrPair,
	parent fsmeta.InodeID,
	refs map[fsmeta.InodeID]uint32,
	inodes map[fsmeta.InodeID]fsmeta.InodeRecord,
	queue *[]fsmeta.InodeID,
	addIssue func(scrubIssue) bool,
) bool {
	dentry := pair.Dentry
	inode := pair.Inode
	if !validScrubName(dentry.Name) {
		if !addIssue(scrubIssue{Kind: issueInvalidDentryName, Parent: parent, Name: dentry.Name, Inode: dentry.Inode}) {
			return false
		}
	}
	if dentry.Parent != parent {
		if !addIssue(scrubIssue{Kind: issueDentryParentMismatch, Parent: parent, Name: dentry.Name, Inode: dentry.Inode, Detail: fmt.Sprintf("dentry_parent=%d", dentry.Parent)}) {
			return false
		}
	}
	if dentry.Inode != inode.Inode {
		if !addIssue(scrubIssue{Kind: issueDentryInodeMismatch, Parent: parent, Name: dentry.Name, Inode: dentry.Inode, Detail: fmt.Sprintf("inode_record=%d", inode.Inode)}) {
			return false
		}
	}
	if dentry.Type != inode.Type {
		if !addIssue(scrubIssue{Kind: issueDentryTypeMismatch, Parent: parent, Name: dentry.Name, Inode: dentry.Inode, Detail: fmt.Sprintf("dentry_type=%s inode_type=%s", dentry.Type, inode.Type)}) {
			return false
		}
	}
	if existing, ok := inodes[inode.Inode]; ok && existing.Type == fsmeta.InodeTypeDirectory && dentry.Type == fsmeta.InodeTypeDirectory {
		if !addIssue(scrubIssue{Kind: issueDirectoryHardlink, Parent: parent, Name: dentry.Name, Inode: inode.Inode}) {
			return false
		}
	}
	refs[inode.Inode]++
	inodes[inode.Inode] = inode
	if inode.Type == fsmeta.InodeTypeDirectory {
		*queue = append(*queue, inode.Inode)
	}
	return true
}

func validScrubName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsAny(name, "/\x00")
}

func sortScrubIssues(issues []scrubIssue) {
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Kind != issues[j].Kind {
			return issues[i].Kind < issues[j].Kind
		}
		if issues[i].Parent != issues[j].Parent {
			return issues[i].Parent < issues[j].Parent
		}
		if issues[i].Name != issues[j].Name {
			return issues[i].Name < issues[j].Name
		}
		return issues[i].Inode < issues[j].Inode
	})
}
