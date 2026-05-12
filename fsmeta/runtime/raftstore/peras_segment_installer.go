package raftstore

import (
	"bytes"
	"context"
	"sync"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

type perasSegmentInstallClient interface {
	InstallPerasSegment(context.Context, []byte, *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error)
}

type perasInstallLane struct {
	owner  *RemotePerasCommitter
	jobs   chan perasInstallRequest
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

type perasInstallRequest struct {
	ctx  context.Context
	job  perasFlushJob
	done chan perasInstallResult
}

type perasInstallResult struct {
	cursor PerasInstallCursor
	err    error
}

func newPerasInstallLane(owner *RemotePerasCommitter, workers int) *perasInstallLane {
	if workers <= 0 {
		workers = 1
	}
	lane := &perasInstallLane{
		owner:  owner,
		jobs:   make(chan perasInstallRequest, workers*4),
		closed: make(chan struct{}),
	}
	lane.wg.Add(workers)
	for range workers {
		go lane.worker()
	}
	return lane
}

func (l *perasInstallLane) install(ctx context.Context, job perasFlushJob) (PerasInstallCursor, error) {
	if l == nil || l.owner == nil {
		return PerasInstallCursor{}, errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.closed:
		return PerasInstallCursor{}, errPerasCommitterClosed
	default:
	}
	done := make(chan perasInstallResult, 1)
	req := perasInstallRequest{ctx: ctx, job: job, done: done}
	select {
	case l.jobs <- req:
	case <-ctx.Done():
		return PerasInstallCursor{}, ctx.Err()
	case <-l.closed:
		return PerasInstallCursor{}, errPerasCommitterClosed
	}
	select {
	case result := <-done:
		return result.cursor, result.err
	case <-ctx.Done():
		return PerasInstallCursor{}, ctx.Err()
	case <-l.closed:
		return PerasInstallCursor{}, errPerasCommitterClosed
	}
}

func (l *perasInstallLane) close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		close(l.closed)
	})
	l.wg.Wait()
}

func (l *perasInstallLane) depth() int {
	if l == nil {
		return 0
	}
	return len(l.jobs)
}

func (l *perasInstallLane) capacity() int {
	if l == nil {
		return 0
	}
	return cap(l.jobs)
}

func (l *perasInstallLane) worker() {
	defer l.wg.Done()
	for {
		select {
		case req := <-l.jobs:
			l.run(req)
		case <-l.closed:
			return
		}
	}
}

func (l *perasInstallLane) run(req perasInstallRequest) {
	if err := req.ctx.Err(); err != nil {
		req.done <- perasInstallResult{err: err}
		return
	}
	ctx, cancel := context.WithCancel(req.ctx)
	defer cancel()
	stop := make(chan struct{})
	go func() {
		select {
		case <-l.closed:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	defer close(stop)
	start := time.Now()
	cursor, err := l.owner.installSegmentWithRetry(ctx, req.job)
	if err == nil {
		l.owner.recordInstallLatency(time.Since(start))
	}
	req.done <- perasInstallResult{cursor: cursor, err: err}
}

func (c *RemotePerasCommitter) installSegmentWithRetry(ctx context.Context, job perasFlushJob) (PerasInstallCursor, error) {
	var last error
	for attempt := 0; attempt <= defaultPerasSegmentInstallRetries; attempt++ {
		cursor, err := c.installer.InstallPerasSegment(ctx, job.scope, job.segment, job.payload, job.digest, job.materialize)
		if err == nil {
			return cursor, nil
		}
		last = err
		if !nokverrors.Retryable(err) || attempt == defaultPerasSegmentInstallRetries {
			break
		}
		c.retryTotal.Add(1)
		delay := defaultPerasSegmentInstallRetryBackoff << attempt
		if delay > defaultPerasSegmentInstallMaxBackoff {
			delay = defaultPerasSegmentInstallMaxBackoff
		}
		if !sleepContext(ctx, delay) {
			return PerasInstallCursor{}, ctx.Err()
		}
	}
	return PerasInstallCursor{}, last
}

func (c *RemotePerasCommitter) submitInstallJob(ctx context.Context, job perasFlushJob) (PerasInstallCursor, error) {
	if c == nil || c.installer == nil {
		return PerasInstallCursor{}, errPerasCommitterInvalid
	}
	if c.installQ != nil {
		return c.installQ.install(ctx, job)
	}
	start := time.Now()
	cursor, err := c.installSegmentWithRetry(ctx, job)
	if err == nil {
		c.recordInstallLatency(time.Since(start))
	}
	return cursor, err
}

type runnerPerasSegmentInstaller struct {
	runner *Runner
	router *fsmetawatch.Router
}

func newRunnerPerasSegmentInstaller(runner *Runner, router *fsmetawatch.Router) *runnerPerasSegmentInstaller {
	return &runnerPerasSegmentInstaller{runner: runner, router: router}
}

func (i *runnerPerasSegmentInstaller) InstallPerasSegment(ctx context.Context, _ compile.AuthorityScope, segment fsperas.PerasSegment, payload []byte, digest [32]byte, materialize bool) (PerasInstallCursor, error) {
	if i == nil || i.runner == nil || i.runner.kv == nil {
		return PerasInstallCursor{}, errPerasCommitterInvalid
	}
	kv, ok := i.runner.kv.(perasSegmentInstallClient)
	if !ok {
		return PerasInstallCursor{}, errPerasCommitterInvalid
	}
	installVersion, err := i.reserveInstallVersion(ctx)
	if err != nil {
		return PerasInstallCursor{}, err
	}
	routingKeys, err := perasSegmentInstallRoutingKeys(segment, materialize)
	if err != nil {
		return PerasInstallCursor{}, err
	}
	var firstCursor PerasInstallCursor
	var firstResp *kvrpcpb.PerasInstallSegmentResponse
	for _, routingKey := range routingKeys {
		resp, err := kv.InstallPerasSegment(ctx, routingKey, &kvrpcpb.PerasInstallSegmentRequest{
			RoutingKey:           runtimeCloneBytes(routingKey),
			SegmentRoot:          append([]byte(nil), segment.Root[:]...),
			SegmentPayloadDigest: append([]byte(nil), digest[:]...),
			SegmentPayload:       append([]byte(nil), payload...),
			InstallVersion:       installVersion,
			MaterializeMvcc:      materialize,
		})
		if err != nil {
			return PerasInstallCursor{}, err
		}
		if resp == nil {
			return PerasInstallCursor{}, errPerasCommitterInvalid
		}
		if keyErr := resp.GetError(); keyErr != nil {
			return PerasInstallCursor{}, runnerKeyError("peras install segment", keyErr)
		}
		if err := validatePerasSegmentInstallResponse(segment, resp); err != nil {
			return PerasInstallCursor{}, err
		}
		cursor := perasInstallCursorFromResponse(resp)
		if !cursor.Valid() {
			return PerasInstallCursor{}, errPerasCommitterInvalid
		}
		if !firstCursor.Valid() {
			firstCursor = cursor
			firstResp = resp
		}
	}
	if !materialize && firstResp != nil {
		i.publishInstalledSegment(segment, firstResp)
	}
	return firstCursor, nil
}

func perasSegmentInstallRoutingKeys(segment fsperas.PerasSegment, materialize bool) ([][]byte, error) {
	if materialize {
		key, err := segment.FirstKey()
		if err != nil {
			return nil, err
		}
		return [][]byte{key}, nil
	}
	return fsperas.PerasSegmentCatalogObjectKeys(segment)
}

func (i *runnerPerasSegmentInstaller) reserveInstallVersion(ctx context.Context) (uint64, error) {
	var last error
	for attempt := 0; attempt <= defaultPerasInstallTimestampRetries; attempt++ {
		version, err := i.runner.ReserveTimestamp(ctx, 1)
		if err == nil {
			return version, nil
		}
		if !nokverrors.Retryable(err) {
			return 0, err
		}
		last = err
		if attempt == defaultPerasInstallTimestampRetries {
			break
		}
		timer := time.NewTimer(defaultPerasInstallTimestampBackoff << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, last
}

func validatePerasSegmentInstallResponse(segment fsperas.PerasSegment, resp *kvrpcpb.PerasInstallSegmentResponse) error {
	if resp == nil {
		return errPerasCommitterInvalid
	}
	if !bytes.Equal(resp.GetSegmentRoot(), segment.Root[:]) {
		return errPerasCommitterInvalid
	}
	stats := segment.Stats()
	if resp.GetOperationCount() != stats.OperationCount ||
		resp.GetEntryCount() != stats.EntryCount ||
		(stats.EntryCount > 0 && resp.GetAppliedEntries() == 0) {
		return errPerasCommitterInvalid
	}
	return nil
}

func perasInstallCursorFromResponse(resp *kvrpcpb.PerasInstallSegmentResponse) PerasInstallCursor {
	if resp == nil {
		return PerasInstallCursor{}
	}
	return PerasInstallCursor{
		RegionID:       resp.GetRegionId(),
		Term:           resp.GetTerm(),
		Index:          resp.GetIndex(),
		InstallVersion: resp.GetCommitVersion(),
	}
}

func (i *runnerPerasSegmentInstaller) publishInstalledSegment(segment fsperas.PerasSegment, resp *kvrpcpb.PerasInstallSegmentResponse) {
	if i == nil || i.router == nil || resp == nil || resp.GetRegionId() == 0 || resp.GetIndex() == 0 {
		return
	}
	commitVersion := resp.GetCommitVersion()
	if commitVersion == 0 {
		return
	}
	cursor := fsmeta.WatchCursor{
		RegionID: resp.GetRegionId(),
		Term:     resp.GetTerm(),
		Index:    resp.GetIndex(),
	}
	for _, entry := range segment.Dentries {
		if len(entry.Key) == 0 {
			continue
		}
		i.router.Publish(fsmeta.WatchEvent{
			Cursor:        cursor,
			CommitVersion: commitVersion,
			Source:        fsmeta.WatchEventSourceCommit,
			Key:           entry.Key,
		})
	}
}
