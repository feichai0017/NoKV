<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
const audiences = [
  {
    icon: 'folder',
    tag: 'DFS frontends',
    title: 'Distributed filesystems',
    body:
      'FUSE / NFS / SMB drivers, JuiceFS- or CubeFS-style services consume fsmeta for inode, dentry, mount, and subtree authority — instead of writing a metadata layer on Redis / TiKV / FoundationDB.',
    primitives: ['ReadDirPlus', 'WatchSubtree', 'SnapshotSubtree', 'RenameSubtree'],
  },
  {
    icon: 'bucket',
    tag: 'Object storage',
    title: 'Namespace for object stores',
    body:
      'S3-compatible gateways consume the same fsmeta for bucket / prefix / version metadata. Fast LIST (server-side ReadDirPlus) and prefix-scoped event streams without client-side stitching.',
    primitives: ['ReadDirPlus (LIST)', 'WatchSubtree', 'SnapshotSubtree', 'RenameSubtree'],
  },
  {
    icon: 'spark',
    tag: 'AI datasets',
    title: 'Dataset & agent workspace',
    body:
      'Checkpoint storms via atomic multi-key AssertionNotExist; point-in-time namespace reads via SnapshotSubtree; prefix-scoped change feeds for training pipelines and shared agent workspaces.',
    primitives: ['SnapshotSubtree', 'WatchSubtree', 'AssertionNotExist', 'ReadDirPlus'],
  },
]
</script>

<template>
  <section class="audiences nokv-section">
    <div class="nokv-section-head">
      <span class="nokv-eyebrow">One substrate · three audiences</span>
      <h2 class="nokv-h2">
        Stop reinventing namespace metadata for every system you ship.
      </h2>
      <p class="nokv-lead">
        Meta Tectonic uses ZippyDB. Google Colossus uses Spanner. DeepSeek 3FS uses
        FoundationDB. Each extracted a metadata layer from its data layer — because
        grafting namespace semantics onto a generic KV is the part that breaks at
        scale. NoKV is that layer, open-sourced and namespace-native.
      </p>
    </div>

    <div class="audience-grid">
      <article v-for="a in audiences" :key="a.title" class="nokv-card audience">
        <div class="audience-icon" :data-icon="a.icon" aria-hidden="true">
          <!-- folder -->
          <svg v-if="a.icon === 'folder'" viewBox="0 0 24 24" width="22" height="22">
            <path
              d="M3 7.5A2.5 2.5 0 0 1 5.5 5h4l2 2h7A2.5 2.5 0 0 1 21 9.5v8A2.5 2.5 0 0 1 18.5 20h-13A2.5 2.5 0 0 1 3 17.5v-10Z"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            />
            <path
              d="M3 10h18"
              stroke="currentColor"
              stroke-width="1.5"
              stroke-linecap="round"
            />
          </svg>
          <!-- bucket -->
          <svg v-else-if="a.icon === 'bucket'" viewBox="0 0 24 24" width="22" height="22">
            <ellipse
              cx="12"
              cy="6"
              rx="8"
              ry="2.4"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            />
            <path
              d="M4 6v11.5C4 19 7.6 20.5 12 20.5S20 19 20 17.5V6"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            />
            <path
              d="M4 11c0 1.4 3.6 2.4 8 2.4s8-1 8-2.4M4 15c0 1.4 3.6 2.4 8 2.4s8-1 8-2.4"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
              opacity=".55"
            />
          </svg>
          <!-- spark -->
          <svg v-else viewBox="0 0 24 24" width="22" height="22">
            <path
              d="M12 3v6m0 6v6M3 12h6m6 0h6"
              stroke="currentColor"
              stroke-width="1.5"
              stroke-linecap="round"
            />
            <path
              d="M6 6l3 3m6 6l3 3M6 18l3-3m6-6l3-3"
              stroke="currentColor"
              stroke-width="1.5"
              stroke-linecap="round"
              opacity=".6"
            />
          </svg>
        </div>

        <span class="audience-tag">{{ a.tag }}</span>
        <h3>{{ a.title }}</h3>
        <p>{{ a.body }}</p>

        <div class="audience-prim">
          <span class="audience-prim-label">Primitives used</span>
          <ul>
            <li v-for="p in a.primitives" :key="p">{{ p }}</li>
          </ul>
        </div>
      </article>
    </div>
  </section>
</template>

<style scoped>
.audience-grid {
  display: grid;
  gap: 22px;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.audience {
  display: flex;
  flex-direction: column;
  min-height: 360px;
}

.audience-icon {
  align-items: center;
  background: var(--vp-c-brand-bg);
  border: 1px solid color-mix(in srgb, var(--nokv-accent) 28%, transparent);
  border-radius: 12px;
  color: var(--nokv-accent);
  display: inline-flex;
  height: 44px;
  justify-content: center;
  margin-bottom: 18px;
  width: 44px;
}

.audience-tag {
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.audience h3 {
  font-size: 1.4rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  margin: 4px 0 14px;
}

.audience p {
  color: var(--vp-c-text-2);
  flex: 1;
  line-height: 1.65;
  margin: 0 0 22px;
}

.audience-prim {
  border-top: 1px dashed var(--vp-c-divider);
  padding-top: 16px;
}

.audience-prim-label {
  color: var(--vp-c-text-3);
  display: block;
  font-family: var(--vp-font-family-mono);
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  margin-bottom: 10px;
  text-transform: uppercase;
}

.audience-prim ul {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  list-style: none;
  margin: 0;
  padding: 0;
}

.audience-prim li {
  background: color-mix(in srgb, var(--nokv-accent) 8%, transparent);
  border: 1px solid color-mix(in srgb, var(--nokv-accent) 18%, transparent);
  border-radius: 8px;
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: 0.78rem;
  padding: 4px 9px;
}

@media (max-width: 960px) {
  .audience-grid {
    grid-template-columns: 1fr;
  }
}
</style>
