<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const primitives = [
  {
    name: 'ReadDirPlus',
    sig: 'ReadDirPlus(mount, parent, limit) → []DentryWithInode',
    desc:
      'Fused directory scan + batch inode fetch under one snapshot. Replaces 1+N client-side round-trips with a single RPC.',
    bullets: ['One snapshot', 'One RTT', 'Skip the N+1 stitch'],
  },
  {
    name: 'WatchSubtree',
    sig: 'WatchSubtree(prefix, cursor) → stream<Event>',
    desc:
      'Prefix-scoped live change feed with ready signal, cursor replay, and flow-control acks. Built on raftstore apply observer + meta/root events.',
    bullets: ['Server-pushed', 'Cursor replay', 'Acked flow-control'],
  },
  {
    name: 'SnapshotSubtree',
    sig: 'SnapshotSubtree(prefix) → ReadVersion',
    desc:
      'MVCC read-version token for point-in-time dataset / bucket / directory reads. Long-lived retention is a GC-layer boundary, not a runtime cost.',
    bullets: ['MVCC token', 'PIT reads', 'No long-tx leak'],
  },
  {
    name: 'RenameSubtree',
    sig: 'RenameSubtree(src, dst) → Result',
    desc:
      'Cross-region atomic namespace move backed by Percolator 2PC + AssertionNotExist. The hard rename that generic KVs cannot deliver cleanly.',
    bullets: ['2PC primary/secondary', 'AssertionNotExist', 'Cross-region atomic'],
  },
]
</script>

<template>
  <section class="prims nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">Server-side primitives</span>
      <h2 class="nokv-h2">
        First-class namespace operations.
        <span class="nokv-gradient-text">No client stitching.</span>
      </h2>
      <p class="nokv-lead">
        Four primitives that constitute the fsmeta API. Wire any DFS, S3, or
        dataset frontend on top of them — NoKV owns the namespace truth, your
        frontend owns the data plane.
      </p>
    </div>

    <div class="prim-grid">
      <article v-for="p in primitives" :key="p.name" class="nokv-card prim">
        <header class="prim-head">
          <h3>{{ p.name }}</h3>
          <span class="prim-tag">primitive</span>
        </header>

        <code class="prim-sig">{{ p.sig }}</code>
        <p class="prim-desc">{{ p.desc }}</p>

        <ul class="prim-bullets">
          <li v-for="b in p.bullets" :key="b">
            <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true">
              <path
                d="M5 12.5l4 4 10-10"
                fill="none"
                stroke="currentColor"
                stroke-width="2.4"
                stroke-linecap="round"
                stroke-linejoin="round"
              />
            </svg>
            {{ b }}
          </li>
        </ul>
      </article>
    </div>
  </section>
</template>

<style scoped>
.prim-grid {
  display: grid;
  gap: 22px;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.prim {
  background: var(--nokv-card-bg-strong);
}

.prim-head {
  align-items: center;
  display: flex;
  justify-content: space-between;
  margin-bottom: 16px;
}

.prim h3 {
  font-family: var(--vp-font-family-mono);
  font-size: 1.18rem;
  font-weight: 700;
  letter-spacing: -0.01em;
  margin: 0;
}

.prim-tag {
  background: var(--vp-c-brand-soft);
  border-radius: 6px;
  color: var(--nokv-accent);
  font-family: var(--vp-font-family-mono);
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  padding: 3px 8px;
  text-transform: uppercase;
}

.prim-sig {
  background: var(--nokv-code-bg);
  border: 1px solid color-mix(in srgb, var(--nokv-accent) 12%, transparent);
  border-radius: 10px;
  color: var(--nokv-code-text);
  display: block;
  font-family: var(--vp-font-family-mono);
  font-size: 0.88rem;
  line-height: 1.55;
  margin-bottom: 16px;
  padding: 10px 14px;
  word-break: break-word;
}

.prim-desc {
  color: var(--vp-c-text-2);
  font-size: 0.98rem;
  line-height: 1.65;
  margin: 0 0 18px;
}

.prim-bullets {
  display: grid;
  gap: 8px;
  list-style: none;
  margin: 0;
  padding: 0;
}

.prim-bullets li {
  align-items: center;
  color: var(--vp-c-text-1);
  display: flex;
  font-size: 0.92rem;
  gap: 8px;
}

.prim-bullets svg {
  color: var(--nokv-accent);
  flex: 0 0 auto;
}

@media (max-width: 800px) {
  .prim-grid {
    grid-template-columns: 1fr;
  }
}
</style>
