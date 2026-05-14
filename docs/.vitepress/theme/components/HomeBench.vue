<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const rows = [
  { name: 'YCSB-A', desc: '50/50 read/update', nokv: 175905, badger: 108232, pebble: 169792 },
  { name: 'YCSB-B', desc: '95/5 read/update', nokv: 525631, badger: 188893, pebble: 137483 },
  { name: 'YCSB-C', desc: '100% read', nokv: 409136, badger: 242463, pebble: 90474 },
  { name: 'YCSB-D', desc: '95% read, 5% insert (latest)', nokv: 632031, badger: 284205, pebble: 198139 },
  { name: 'YCSB-E', desc: '95% scan, 5% insert', nokv: 45620, badger: 15027, pebble: 40793 },
  { name: 'YCSB-F', desc: 'read-modify-write', nokv: 157732, badger: 84601, pebble: 122192 },
]

const fmt = (n: number) => n.toLocaleString('en-US')

const max = (row: { nokv: number; badger: number; pebble: number }) =>
  Math.max(row.nokv, row.badger, row.pebble)
</script>

<template>
  <section class="bench nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">Headline evidence</span>
      <h2 class="nokv-h2">
        The underlying KV engine outruns its peers — by design.
      </h2>
      <p class="nokv-lead">
        Apple M3 Pro · <code>records=1M</code> · <code>ops=1M</code> ·
        <code>value_size=1000</code> · <code>conc=16</code>. NoKV is built on its
        own LSM (WAL + ART memtable + leveled compaction + landing buffer), measured
        against Badger and Pebble on the same machine.
      </p>
    </div>

    <div class="bench-card nokv-card">
      <div class="bench-table-wrap">
        <table class="bench-table">
          <thead>
            <tr>
              <th class="th-name">Workload</th>
              <th class="th-desc">Description</th>
              <th class="th-num th-best">NoKV</th>
              <th class="th-num">Badger</th>
              <th class="th-num">Pebble</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="r in rows" :key="r.name">
              <td class="td-name">{{ r.name }}</td>
              <td class="td-desc">{{ r.desc }}</td>
              <td class="td-num td-best" :class="{ winner: r.nokv === max(r) }">
                {{ fmt(r.nokv) }}
              </td>
              <td class="td-num" :class="{ winner: r.badger === max(r) }">
                {{ fmt(r.badger) }}
              </td>
              <td class="td-num" :class="{ winner: r.pebble === max(r) }">
                {{ fmt(r.pebble) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p class="bench-fine">
        ops/sec · single-node localhost, not multi-host production · full latency in
        <code>benchmark/README.md</code>
      </p>
    </div>
  </section>
</template>

<style scoped>
.bench-card {
  padding: 0;
  overflow: hidden;
}

.bench-table-wrap {
  overflow-x: auto;
}

.bench-table {
  border-collapse: collapse;
  font-feature-settings: 'tnum';
  width: 100%;
}

.bench-table thead th {
  background: color-mix(in srgb, var(--vp-c-bg-soft) 70%, transparent);
  border-bottom: 1px solid var(--vp-c-divider);
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 0.78rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  padding: 16px 18px;
  text-align: left;
  text-transform: uppercase;
}

.bench-table th.th-num,
.bench-table td.td-num {
  text-align: right;
}

.bench-table th.th-best {
  color: var(--nokv-accent);
}

.bench-table tbody tr + tr {
  border-top: 1px solid var(--vp-c-divider);
}

.bench-table td {
  font-size: 0.96rem;
  padding: 16px 18px;
}

.td-name {
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-weight: 700;
}

.td-desc {
  color: var(--vp-c-text-2);
}

.td-num {
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-variant-numeric: tabular-nums;
}

.td-best {
  color: var(--vp-c-text-1);
  font-weight: 600;
}

.td-num.winner {
  background: color-mix(in srgb, var(--nokv-accent) 8%, transparent);
  color: var(--nokv-accent);
  font-weight: 700;
  position: relative;
}

.td-num.winner::before {
  content: '▲';
  font-size: 0.65rem;
  margin-right: 6px;
  vertical-align: middle;
}

.bench-fine {
  border-top: 1px solid var(--vp-c-divider);
  color: var(--vp-c-text-3);
  font-size: 0.84rem;
  margin: 0;
  padding: 14px 24px;
}

.bench-fine code {
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92em;
}

.nokv-lead code {
  background: var(--vp-c-brand-bg);
  border-radius: 6px;
  color: var(--nokv-accent);
  font-family: var(--vp-font-family-mono);
  font-size: 0.92em;
  padding: 1px 6px;
}
</style>
