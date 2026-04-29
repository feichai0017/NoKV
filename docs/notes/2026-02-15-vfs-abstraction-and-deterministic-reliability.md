# NoKV VFS abstraction: a storage contract that crosses OS boundaries, plus deterministic fault simulation

NoKV's VFS (Virtual File System) layer is not there to add complexity. It is the core fortress that lets the engine deliver **deterministic reliability**. By cleanly decoupling storage semantics from OS-specific details, NoKV achieves cross-platform atomic-semantic guarantees and high-intensity fault-injection testing.

---

## 1. Why does an industrial storage engine need a VFS?

When you build a storage engine directly on the native `os` package, three problems are fatal:
1.  **Missing atomic semantics**: an LSM engine relies on the atomicity of `Rename` to update the Manifest. But across OSes, whether `Rename` overwrites an existing file and whether it's actually atomic varies enormously.
2.  **Test opacity**: how do you verify what happens when the disk loses power mid-`Sync`? How do you simulate a bad sector without physically pulling a drive?
3.  **Limited extensibility**: if you later want to plug in a **distributed filesystem (HDFS/S3)** or run a **pure in-memory mode**, no VFS means you rewrite the whole storage core.

NoKV abstracts all I/O behavior into a unified contract through the `vfs.FS` and `vfs.File` interfaces (in `vfs/vfs.go`).

---

## 2. FaultFS: the precision "fault scalpel"

`vfs/faultfs.go` is the reliability testing tool we are most proud of. It wraps the standard filesystem in a decorator pattern and lets test cases inject extreme faults programmatically.

### 2.1 Fault policies (FaultPolicy)
Developers can describe arbitrarily complex fault scenarios and observe how the engine recovers:
*   **FailOnce**: trigger a single error on a specific file operation (simulating transient I/O jitter).
*   **FailOnNth**: trigger a fault on the Nth operation (e.g., the 100th `Write`). This is critical for verifying the idempotency of **crash recovery**.
*   **FailOnOp**: trigger only on heavyweight ops that change filesystem metadata, like `Sync` or `Truncate`.

### 2.2 How it works
```go
// faultFile's WriteAt implementation
func (f *faultFile) WriteAt(p []byte, off int64) (int, error) {
    // 1. Before the real I/O, run the policy precheck.
    if err := f.fs.before(OpWriteAt, f.name); err != nil {
        return 0, err // simulated fault
    }
    // 2. Execute the actual OS call.
    return f.File.WriteAt(p, off)
}
```
With this hook, NoKV's test suite has successfully simulated:
*   Manifest write hits a "disk full" error mid-write.
*   SSTable generation completes the file but `Sync` then fails.
*   WAL hits a permission error mid-rollback.

---

## 3. Smoothing over cross-platform semantics: atomic-rename protocol

The lifeblood of any LSM engine is **atomic Manifest replacement**. NoKV pushes this contract down into the VFS layer and tunes it per-OS.

### 3.1 Linux: `RenameNoReplace`
On Linux, NoKV leverages the `unix.RENAME_NOREPLACE` flag.
*   **Design value**: if the target Manifest already exists, the rename atomically fails instead of overwriting. This eliminates the class of bugs where an abnormal restart silently overwrites valid metadata.

### 3.2 Darwin: emulated support
macOS does not natively support `RENAME_NOREPLACE`. NoKV's VFS emulates the same behavior using `getattrlist` plus an atomic existence check, so a developer on Mac sees exactly the same logical contract as the Linux production environment.

---

## 4. Performance unlocked: the PRead/PWrite concurrency contract

VFS isn't just for reliability — it also enables high-throughput **lock-free concurrent reads**.

*   **PRead semantics (`ReadAt`)**: `vfs.File` mandates a `ReadAt` implementation.
*   **Contention-free read**: in the ValueLog read path, many query goroutines can share one file descriptor and call `ReadAt` concurrently. Because `ReadAt` carries its own offset, it doesn't need a file-handle-level mutex like `Seek + Read` would.
*   **Result**: on a multi-core machine reading large values, NoKV throughput scales linearly with the number of CPU cores.

---

## 5. Comparison with peer storage engines

| Property | NoKV | Pebble (CockroachDB) | RocksDB |
| :--- | :--- | :--- | :--- |
| **VFS architecture** | Slim interface + decorator injection | Deep integration (errorfs) | Complex Env/FileSystem abstraction |
| **Fault-injection power** | Strong (path/op-level counters) | Very strong (rich counter strategies) | Medium (depends on Env hook points) |
| **Concurrent-read contract** | Mandatory PRead/PWrite | Heavily optimized PRead | OS-dependent |
| **Cross-platform atomicity** | Smooths Linux/Darwin differences | Guaranteed by Go runtime | Plugin-specific |

**Bottom line**: VFS is not overhead — it is an attitude of **"being responsible for every disk operation."** Through VFS, NoKV converges complex low-level syscalls and unpredictable hardware faults into a predictable, testable, provable deterministic model.
