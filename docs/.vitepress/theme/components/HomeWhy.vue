<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const reasons = [
  {
    n: '01',
    title: 'Server-side namespace primitives',
    body:
      'ReadDirPlus returns one directory + N child stats in a single round-trip. Client-side stitching on a generic KV does 1+N RTTs and pays a 42× end-to-end latency penalty — measured on the same NoKV cluster.',
    pill: 'fsmeta',
  },
  {
    n: '02',
    title: 'Namespace correctness is its own class',
    body:
      'Subtree authority handoff, mount lifecycle, snapshot epoch, and quota fence have a formal model. Eunomia is TLC-model-checked under finite bounds — a property no general-purpose KV provides because it is not a general-purpose KV property.',
    pill: 'TLA+ / TLC',
  },
  {
    n: '03',
    title: 'Bring your own data plane',
    body:
      'NoKV does not store object bytes, chunk data, or POSIX file content. You wire it under a FUSE driver, an S3 gateway, or a dataset SDK; NoKV is the namespace truth those frontends consume.',
    pill: 'Apache-2.0',
  },
]
</script>

<template>
  <section class="why nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">Why it is its own layer</span>
      <h2 class="nokv-h2">
        A generic KV is the wrong shape for namespace metadata.
      </h2>
      <p class="nokv-lead">
        Three structural reasons it deserves a purpose-built substrate instead of a
        feature you bolt onto Redis / etcd / FoundationDB / TiKV.
      </p>
    </div>

    <div class="why-grid">
      <article v-for="r in reasons" :key="r.n" class="nokv-card why-card">
        <div class="why-head">
          <span class="why-num">{{ r.n }}</span>
          <span class="nokv-pill">{{ r.pill }}</span>
        </div>
        <h3>{{ r.title }}</h3>
        <p>{{ r.body }}</p>
      </article>
    </div>
  </section>
</template>

<style scoped>
.why-grid {
  display: grid;
  gap: 22px;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.why-card {
  display: flex;
  flex-direction: column;
  min-height: 280px;
}

.why-head {
  align-items: center;
  display: flex;
  justify-content: space-between;
  margin-bottom: 18px;
}

.why-num {
  background: linear-gradient(135deg, var(--nokv-accent) 0%, var(--nokv-accent-2) 100%);
  -webkit-background-clip: text;
  background-clip: text;
  color: transparent;
  font-family: var(--vp-font-family-mono);
  font-size: 2.6rem;
  font-weight: 800;
  letter-spacing: -0.04em;
  line-height: 1;
}

.why-card h3 {
  font-size: 1.28rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  margin: 0 0 14px;
}

.why-card p {
  color: var(--vp-c-text-2);
  font-size: 0.98rem;
  line-height: 1.65;
  margin: 0;
}

@media (max-width: 960px) {
  .why-grid {
    grid-template-columns: 1fr;
  }
}
</style>
