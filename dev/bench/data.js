window.BENCHMARK_DATA = {
  "lastUpdate": 1772758496365,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "feichai0017",
            "username": "feichai0017"
          },
          "committer": {
            "email": "songguocheng348@gmail.com",
            "name": "feichai0017",
            "username": "feichai0017"
          },
          "distinct": true,
          "id": "35eca82069d565b8a5e26a25851a6b6af68d623d",
          "message": "chore: align docs and fmt workflow with current APIs",
          "timestamp": "2026-03-06T11:54:08+11:00",
          "tree_id": "023aa5c15816fce58a108ccb90e55ca180020061",
          "url": "https://github.com/feichai0017/NoKV/commit/35eca82069d565b8a5e26a25851a6b6af68d623d"
        },
        "date": 1772758494090,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkTouch",
            "value": 23.74,
            "unit": "ns/op",
            "extra": "50066800 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel",
            "value": 58.23,
            "unit": "ns/op",
            "extra": "20215077 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp",
            "value": 20,
            "unit": "ns/op",
            "extra": "59635338 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency",
            "value": 16.84,
            "unit": "ns/op",
            "extra": "72339235 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN",
            "value": 20690141,
            "unit": "ns/op",
            "extra": "60 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow",
            "value": 74.83,
            "unit": "ns/op",
            "extra": "16012785 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay",
            "value": 49515,
            "unit": "ns/op",
            "extra": "23938 times\n4 procs"
          }
        ]
      }
    ]
  }
}