from __future__ import annotations

import argparse
import gc
import json
import platform
import sys
import time
import tracemalloc
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

from langchain_core.messages import HumanMessage
from typing_extensions import TypedDict

from langgraph.channels.delta import DeltaChannel
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.nokv import (
    CheckpointBodyStore,
    InodeType,
    NoKVCheckpointSaver,
    NoKVFsMetaClient,
)
from langgraph.checkpoint.nokv._bench_metrics import (
    DELTA_HISTORY_PHASE,
    GET_STATE_PHASE,
    STORAGE_COUNT_PHASE,
    WRITE_PHASE,
    InstrumentedCheckpointBodyStore,
    InstrumentedFsMetaClient,
    InstrumentedSaverMetrics,
    InstrumentedSerde,
    PhaseTracker,
)
from langgraph.graph import END, StateGraph
from langgraph.graph.message import _messages_delta_reducer

try:
    from langgraph.checkpoint.postgres import PostgresSaver

    _POSTGRES_AVAILABLE = True
except ImportError:
    PostgresSaver = None
    _POSTGRES_AVAILABLE = False


_HUMAN_TEMPLATE = (
    "I need help understanding the implications of {topic} on our system "
    "architecture. Specifically, I'm concerned about how this interacts with "
    "our existing {concern} and whether we need to refactor the {component} "
    "layer before proceeding."
)

_TOPICS = [
    "distributed tracing",
    "eventual consistency",
    "schema migration",
    "backpressure handling",
    "idempotency guarantees",
]
_CONCERNS = ["concurrency model", "retry semantics", "ordering guarantees"]
_COMPONENTS = ["persistence", "ingestion", "routing"]

SCENARIOS: dict[str, list[int]] = {
    "k1_freq50": [50],
    "k3_freq50_uniform": [50, 50, 50],
    "k3_freq_mixed": [50, 200, 1000],
    "k8_freq50_uniform": [50] * 8,
    "k8_freq_mixed": [25, 50, 100, 200, 500, 1000, 1000, 1000],
}


@dataclass
class SaverContext:
    label: str
    saver: Any
    close: Callable[[], None]
    storage: Callable[[], dict[str, Any]]
    extra: Callable[[], dict[str, Any]]
    phase: Callable[[str], Any]
    saver_metrics: InstrumentedSaverMetrics | None = None


def main() -> int:
    args = _parse_args()
    run_id = args.run_id or time.strftime("lg-nokv-%Y%m%d-%H%M%S")
    output = args.output or (
        Path("artifacts")
        / "langgraph-nokv-bench"
        / f"delta-channel-{run_id}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    scenarios = _select_scenarios(args.scenarios)
    turn_counts = _parse_int_csv(args.turn_counts)
    savers = _parse_csv(args.savers)

    rows: list[dict[str, Any]] = []
    environment = _environment(args)
    print("DeltaChannel benchmark - LangGraph official workload")
    print("=" * 96)
    print(
        f"run_id={run_id} savers={','.join(savers)} turns={','.join(map(str, turn_counts))}"
    )
    _write_metrics(output, run_id, environment, rows)

    for saver_name in savers:
        for scenario_name, freqs in scenarios:
            for turns in turn_counts:
                thread_id = _thread_id(run_id, saver_name, scenario_name, turns)
                print(
                    f"starting saver={saver_name} scenario={scenario_name} turns={turns}",
                    flush=True,
                )
                context = _open_saver(args, saver_name, run_id, thread_id)
                before_metrics = _fetch_json(args.fsmeta_metrics_url)
                try:
                    row = _run_scenario(
                        scenario_name=scenario_name,
                        freqs=freqs,
                        turns=turns,
                        saver_label=context.label,
                        checkpointer=context.saver,
                        thread_id=thread_id,
                        read_repeats=args.read_repeats,
                        phase=context.phase,
                        saver_metrics=context.saver_metrics,
                    )
                    after_metrics = _fetch_json(args.fsmeta_metrics_url)
                    row.update(context.storage())
                    row.update(context.extra())
                    row["fsmeta_metrics_delta"] = _numeric_delta(
                        before_metrics, after_metrics
                    )
                    rows.append(row)
                    _print_row(row)
                    _write_metrics(output, run_id, environment, rows)
                finally:
                    context.close()

    _write_metrics(output, run_id, environment, rows)
    print(f"\nmetrics_json={output}")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run LangGraph DeltaChannel benchmark against checkpoint savers."
    )
    parser.add_argument(
        "--savers",
        default="nokv",
        help=(
            "Comma-separated saver list: nokv,nokv-parent-chain,memory,postgres. "
            "nokv uses the fsmeta delta index; nokv-parent-chain disables it."
        ),
    )
    parser.add_argument(
        "--scenarios",
        default="official",
        help="official, smoke, or comma-separated scenario ids.",
    )
    parser.add_argument("--turn-counts", default="100,500")
    parser.add_argument("--read-repeats", type=int, default=5)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--nokv-target", default="127.0.0.1:8090")
    parser.add_argument("--nokv-mount", default="fsmeta-bench")
    parser.add_argument(
        "--nokv-body-root",
        type=Path,
        default=Path("artifacts/langgraph-nokv-bench/body"),
    )
    parser.add_argument("--nokv-ready-timeout", type=float, default=30.0)
    parser.add_argument(
        "--fsmeta-metrics-url",
        default="http://127.0.0.1:9400/debug/vars",
    )
    parser.add_argument(
        "--postgres-uri",
        default="postgres://postgres@localhost:5432/postgres?sslmode=disable",
    )
    return parser.parse_args()


def _select_scenarios(spec: str) -> list[tuple[str, list[int]]]:
    if spec == "official":
        return list(SCENARIOS.items())
    if spec == "smoke":
        return [("k1_freq50", SCENARIOS["k1_freq50"])]
    selected: list[tuple[str, list[int]]] = []
    for name in _parse_csv(spec):
        try:
            selected.append((name, SCENARIOS[name]))
        except KeyError as exc:
            raise SystemExit(f"unknown scenario {name!r}") from exc
    return selected


def _open_saver(
    args: argparse.Namespace, saver_name: str, run_id: str, thread_id: str
) -> SaverContext:
    if saver_name == "memory":
        saver = MemorySaver()
        return SaverContext(
            label="memory",
            saver=saver,
            close=lambda: None,
            storage=lambda: {"storage_bytes": _inmemory_blob_bytes(saver)},
            extra=lambda: {},
            phase=lambda _phase: nullcontext(),
        )

    if saver_name in {"nokv", "nokv-parent-chain"}:
        enable_delta_index = saver_name == "nokv"
        body_root = args.nokv_body_root / run_id / thread_id
        phase_tracker = PhaseTracker()
        client = InstrumentedFsMetaClient(
            NoKVFsMetaClient(args.nokv_target),
            phase_tracker=phase_tracker,
        )
        client.wait_ready(timeout=args.nokv_ready_timeout)
        body_store = InstrumentedCheckpointBodyStore(
            CheckpointBodyStore.from_local_path(body_root),
            phase_tracker=phase_tracker,
        )
        saver = NoKVCheckpointSaver(
            fsmeta_client=client,  # type: ignore[arg-type]
            mount=args.nokv_mount,
            body_store=body_store,  # type: ignore[arg-type]
            enable_delta_index=enable_delta_index,
        )
        saver.serde = InstrumentedSerde(saver.serde, phase_tracker=phase_tracker)
        saver_metrics = InstrumentedSaverMetrics(phase_tracker=phase_tracker)
        saver_metrics.wrap_nokv_saver(saver)
        saver._benchmark_saver_metrics = saver_metrics

        def storage() -> dict[str, Any]:
            with phase_tracker.phase(STORAGE_COUNT_PHASE):
                tree_counts = _count_layout_tree(
                    client=client,
                    mount=args.nokv_mount,
                    path=saver.layout.thread_dir(thread_id),
                )
            body = _body_store_stats(body_root)
            return {
                "storage_bytes": body["body_bytes"],
                **body,
                **tree_counts,
            }

        return SaverContext(
            label="nokv-delta-index" if enable_delta_index else "nokv-parent-chain",
            saver=saver,
            close=client.close,
            storage=storage,
            extra=lambda: {
                "delta_index_enabled": enable_delta_index,
                "fsmeta_client": client.to_json(),
                "body_store_metrics": body_store.to_json(),
                "serde_metrics": saver.serde.to_json(),
                "saver_metrics": saver_metrics.to_json(),
            },
            phase=phase_tracker.phase,
            saver_metrics=saver_metrics,
        )

    if saver_name == "postgres":
        if not _POSTGRES_AVAILABLE or PostgresSaver is None:
            raise SystemExit("postgres saver is not installed")
        postgres_context = PostgresSaver.from_conn_string(args.postgres_uri)
        saver = postgres_context.__enter__()
        phase_tracker = PhaseTracker()
        saver.serde = InstrumentedSerde(saver.serde, phase_tracker=phase_tracker)
        saver_metrics = InstrumentedSaverMetrics(phase_tracker=phase_tracker)
        saver_metrics.wrap_postgres_saver(saver)
        saver._benchmark_saver_metrics = saver_metrics
        try:
            saver.setup()
            _clear_postgres_thread(saver, thread_id)
        except BaseException:
            postgres_context.__exit__(*sys.exc_info())
            raise
        return SaverContext(
            label="postgres",
            saver=saver,
            close=lambda: _close_postgres_saver(
                postgres_context, saver, thread_id
            ),
            storage=lambda: {
                "storage_bytes": _postgres_storage_bytes(saver, thread_id)
            },
            extra=lambda: {
                "serde_metrics": saver.serde.to_json(),
                "saver_metrics": saver_metrics.to_json(),
            },
            phase=phase_tracker.phase,
            saver_metrics=saver_metrics,
        )

    raise SystemExit(f"unknown saver {saver_name!r}")


def _run_scenario(
    *,
    scenario_name: str,
    freqs: list[int],
    turns: int,
    saver_label: str,
    checkpointer: Any,
    thread_id: str,
    read_repeats: int,
    phase: Callable[[str], Any],
    saver_metrics: InstrumentedSaverMetrics | None = None,
) -> dict[str, Any]:
    state_cls = _make_state_cls(freqs)
    graph = _make_graph(state_cls, len(freqs), checkpointer)
    config = {"configurable": {"thread_id": thread_id}}

    start = time.perf_counter()
    with phase(WRITE_PHASE):
        for _ in range(turns):
            graph.invoke({}, config)
    write_elapsed = time.perf_counter() - start

    gc.collect()
    tracemalloc.start()
    read_start = time.perf_counter()
    with phase(GET_STATE_PHASE):
        for _ in range(read_repeats):
            graph.get_state(config)
    read_elapsed = (time.perf_counter() - read_start) / read_repeats

    delta_channels = [f"ch{i}" for i in range(len(freqs))]
    delta_read_start = time.perf_counter()
    with phase(DELTA_HISTORY_PHASE):
        for _ in range(read_repeats):
            history = checkpointer.get_delta_channel_history(
                config=config,
                channels=delta_channels,
            )
            if saver_metrics is not None:
                saver_metrics.record_delta_history_result(history)
    delta_read_elapsed = (time.perf_counter() - delta_read_start) / read_repeats
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return {
        "saver": saver_label,
        "scenario": scenario_name,
        "K": len(freqs),
        "freqs": freqs,
        "turns": turns,
        "thread_id": thread_id,
        "write_total_s": write_elapsed,
        "write_per_invoke_ms": write_elapsed / turns * 1000,
        "read_avg_ms": read_elapsed * 1000,
        "read_repeats": read_repeats,
        "delta_history_avg_ms": delta_read_elapsed * 1000,
        "delta_history_repeats": read_repeats,
        "delta_channels": delta_channels,
        "peak_mem_bytes": peak_bytes,
    }


def _make_state_cls(freqs: list[int]) -> type:
    fields: dict[str, Any] = {}
    for i, freq in enumerate(freqs):
        fields[f"ch{i}"] = Annotated[
            list, DeltaChannel(_messages_delta_reducer, snapshot_frequency=freq)
        ]
    return TypedDict("_BenchState_" + "_".join(str(f) for f in freqs), fields)


def _make_graph(state_cls: type, channel_count: int, checkpointer: Any) -> Any:
    def fanout(state: Any) -> dict[str, Any]:
        current = max(len(state.get(f"ch{i}", [])) for i in range(channel_count))
        return {
            f"ch{i}": [
                HumanMessage(content=_human_content(current), id=f"c{i}_{current}")
            ]
            for i in range(channel_count)
        }

    graph = StateGraph(state_cls)
    graph.add_node("fanout", fanout)
    graph.set_entry_point("fanout")
    graph.add_edge("fanout", END)
    return graph.compile(checkpointer=checkpointer)


def _human_content(index: int) -> str:
    return _HUMAN_TEMPLATE.format(
        topic=_TOPICS[index % len(_TOPICS)],
        concern=_CONCERNS[index % len(_CONCERNS)],
        component=_COMPONENTS[index % len(_COMPONENTS)],
    )


def _body_store_stats(root: Path) -> dict[str, int]:
    files = [path for path in root.rglob("*") if path.is_file()]
    return {
        "body_bytes": sum(path.stat().st_size for path in files),
        "body_files": len(files),
    }


def _count_layout_tree(
    *,
    mount: str,
    path: tuple[str, ...],
    target: str | None = None,
    client: Any | None = None,
) -> dict[str, int]:
    close_client = False
    if client is None:
        if target is None:
            raise ValueError("target or client is required")
        client = NoKVFsMetaClient(target)
        close_client = True
    try:
        inode = _resolve_path(client, mount, path)
        counts = {"fsmeta_entries": 0, "fsmeta_dirs": 0, "fsmeta_files": 0}
        _count_tree(client, mount, inode, counts)
        return counts
    except FileNotFoundError:
        return {"fsmeta_entries": 0, "fsmeta_dirs": 0, "fsmeta_files": 0}
    finally:
        if close_client:
            client.close()


def _count_tree(
    client: NoKVFsMetaClient, mount: str, parent: int, counts: dict[str, int]
) -> None:
    start_after = ""
    while True:
        page = client.read_dir_plus(
            mount=mount, parent=parent, start_after=start_after, limit=1024
        )
        for pair in page:
            counts["fsmeta_entries"] += 1
            if pair.inode.type == InodeType.DIRECTORY:
                counts["fsmeta_dirs"] += 1
                _count_tree(client, mount, pair.dentry.inode, counts)
            elif pair.inode.type == InodeType.FILE:
                counts["fsmeta_files"] += 1
        if len(page) < 1024:
            return
        start_after = page[-1].dentry.name


def _resolve_path(client: NoKVFsMetaClient, mount: str, path: tuple[str, ...]) -> int:
    parent = 1
    for name in path:
        try:
            parent = client.lookup(mount=mount, parent=parent, name=name).inode
        except Exception as exc:
            raise FileNotFoundError(name) from exc
    return parent


def _inmemory_blob_bytes(saver: MemorySaver) -> int:
    return sum(
        len(blob) for (_, _, _, _), (_, blob) in saver.blobs.items() if blob is not None
    )


def _postgres_storage_bytes(saver: Any, thread_id: str) -> int:
    sql = """
    SELECT COALESCE(SUM(pg_column_size(c.*)), 0)
         + COALESCE((SELECT SUM(pg_column_size(b.*)) FROM checkpoint_blobs b
                     WHERE b.thread_id = %s), 0)
         + COALESCE((SELECT SUM(pg_column_size(w.*)) FROM checkpoint_writes w
                     WHERE w.thread_id = %s), 0)
         AS total
    FROM checkpoints c
    WHERE c.thread_id = %s
    """
    with saver._cursor() as cur:
        cur.execute(sql, (thread_id, thread_id, thread_id))
        row = cur.fetchone()
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(row.get("total") or 0)
    return int(row[0] or 0)


def _clear_postgres_thread(saver: Any, thread_id: str) -> None:
    with saver._cursor() as cur:
        for table in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
            cur.execute(f"DELETE FROM {table} WHERE thread_id = %s", (thread_id,))


def _close_postgres_saver(context: Any, saver: Any, thread_id: str) -> None:
    try:
        _clear_postgres_thread(saver, thread_id)
    finally:
        context.__exit__(None, None, None)


def _fetch_json(url: str) -> Any:
    if not url:
        return {}
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return {}


def _numeric_delta(before: Any, after: Any) -> Any:
    if isinstance(before, dict) and isinstance(after, dict):
        delta: dict[str, Any] = {}
        for key in sorted(set(before) | set(after)):
            item = _numeric_delta(before.get(key), after.get(key))
            if item not in ({}, 0, 0.0, None):
                delta[key] = item
        return delta
    if isinstance(before, int | float) and isinstance(after, int | float):
        return after - before
    return None


def _environment(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "packages": {
            name: _package_version(name)
            for name in (
                "grpcio",
                "langgraph",
                "langgraph-checkpoint",
                "langgraph-checkpoint-nokv",
            )
        },
        "nokv_target": args.nokv_target,
        "nokv_mount": args.nokv_mount,
        "fsmeta_metrics_url": args.fsmeta_metrics_url,
        "benchmark_schema": "delta-channel-v2",
    }


def _package_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _print_row(row: dict[str, Any]) -> None:
    print(
        f"{row['saver']:<8} {row['scenario']:<20} turns={row['turns']:<4} "
        f"write_ms={row['write_per_invoke_ms']:.2f} "
        f"read_ms={row['read_avg_ms']:.2f} "
        f"delta_ms={row['delta_history_avg_ms']:.2f} "
        f"storage={_fmt_bytes(int(row.get('storage_bytes', -1)))} "
        f"peak={_fmt_bytes(int(row['peak_mem_bytes']))}",
        flush=True,
    )


def _write_metrics(
    output: Path, run_id: str, environment: dict[str, Any], rows: list[dict[str, Any]]
) -> None:
    document = {
        "run_id": run_id,
        "updated_unix_ns": time.time_ns(),
        "environment": environment,
        "rows": rows,
    }
    output.write_text(json.dumps(document, indent=2, sort_keys=True), encoding="utf-8")


def _fmt_bytes(value: int) -> str:
    if value < 0:
        return "n/a"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f} MB"
    if value >= 1_000:
        return f"{value / 1_000:.1f} KB"
    return f"{value} B"


def _thread_id(run_id: str, saver: str, scenario: str, turns: int) -> str:
    return f"{run_id}-{saver}-{scenario}-{turns}-{uuid4().hex[:8]}"


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_int_csv(value: str) -> list[int]:
    items = [int(item) for item in _parse_csv(value)]
    if not items:
        raise SystemExit("at least one turn count is required")
    return items


if __name__ == "__main__":
    raise SystemExit(main())
