# CLI 工具 (`cmd/nokv`)

`nokv` CLI 提供对运行中或离线的 NoKV 实例进行巡检的能力，覆盖指标、Manifest 以及 ValueLog 状态。

## 安装

```bash
go install ./cmd/nokv
```

## 子命令

- `nokv stats --workdir <dir>`  
  打开指定 `WorkDir`，拉取一次离线指标快照。若服务已对外暴露 `expvar`，可使用
  `nokv stats --expvar http://host:port/debug/vars` 直接读取线上指标。支持 `--json` 输出 JSON。
  输出包含 flush/compaction backlog、ValueLog 状态，以及 `Txns.{Active,Started,Committed,Conflicts}` 等事务指标。

- `nokv manifest --workdir <dir>`  
  解析 `CURRENT` 与 Manifest，输出各层文件统计、ValueLog Head 以及段状态。支持 `--json`。

- `nokv vlog --workdir <dir>`  
  查看 ValueLog 段列表、活跃段及写入头指针，便于确认 GC 是否生效，同样支持 `--json`。

## 示例

```bash
nokv stats --workdir ./work_test
nokv manifest --workdir ./work_test --json
nokv vlog --workdir ./work_test
```

输出内容可用于快速核对 flush backlog、Compaction backlog、ValueLog GC 进度等关键指标。结合 `expvar`
采集，可以将 CLI 融入巡检脚本或运维流水线。
