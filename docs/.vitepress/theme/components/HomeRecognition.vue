<script setup lang="ts">
import { withBase } from 'vitepress'

const featured = [
  {
    key: 'cncf',
    name: 'CNCF Landscape',
    headline: 'Listed under Cloud Native Storage runtime',
    detail: 'Cloud Native Computing Foundation · projects-and-products',
    logo: '/img/recognition/cncf.svg',
    href: 'https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv',
  },
  {
    key: 'dbdb',
    name: 'dbdb.io',
    headline: 'Profiled in the database of databases',
    detail: 'CMU-DB · database systems catalog',
    logo: '/img/recognition/dbdb.svg',
    href: 'https://dbdb.io/db/nokv',
  },
]

const chips = [
  {
    name: 'Awesome Go',
    label: 'Featured under databases-implemented-in-go',
    href: 'https://github.com/avelino/awesome-go#databases-implemented-in-go',
  },
  {
    name: 'pkg.go.dev',
    label: 'Go package reference',
    href: 'https://pkg.go.dev/github.com/feichai0017/NoKV',
  },
  {
    name: 'DeepWiki',
    label: 'Ask anything about the codebase',
    href: 'https://deepwiki.com/feichai0017/NoKV',
  },
]
</script>

<template>
  <section class="recognition nokv-section nokv-section--tight">
    <div class="nokv-section-head nokv-section-head--center">
      <span class="nokv-eyebrow">Recognition · listed in</span>
      <h2 class="nokv-h2">
        A documented spot in the
        <span class="nokv-gradient-text">storage canon.</span>
      </h2>
    </div>

    <div class="featured-row">
      <a
        v-for="f in featured"
        :key="f.key"
        :href="f.href"
        target="_blank"
        rel="noreferrer"
        class="featured-card"
        :class="`featured-card--${f.key}`"
      >
        <div class="featured-mark">
          <img :src="withBase(f.logo)" :alt="`${f.name} logo`" />
        </div>

        <div class="featured-meta">
          <span class="featured-eyebrow">{{ f.name }}</span>
          <strong>{{ f.headline }}</strong>
          <span class="featured-detail">{{ f.detail }}</span>
        </div>

        <span class="featured-arrow" aria-hidden="true">
          <svg viewBox="0 0 24 24" width="22" height="22">
            <path
              d="M7 17 17 7M9 7h8v8"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            />
          </svg>
        </span>
      </a>
    </div>

    <div class="chip-section">
      <span class="chip-label">Also listed in</span>
      <div class="chip-row">
        <a
          v-for="c in chips"
          :key="c.name"
          :href="c.href"
          target="_blank"
          rel="noreferrer"
          class="chip"
        >
          <strong>{{ c.name }}</strong>
          <span>{{ c.label }}</span>
          <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true">
            <path
              d="M7 17 17 7M9 7h8v8"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            />
          </svg>
        </a>
      </div>
    </div>
  </section>
</template>

<style scoped>
.recognition {
  position: relative;
}

.featured-row {
  display: grid;
  gap: 22px;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  margin-bottom: 36px;
}

.featured-card {
  align-items: center;
  background: var(--nokv-card-bg);
  border: 1px solid var(--nokv-card-border);
  border-radius: 18px;
  box-shadow: var(--nokv-card-shadow);
  color: var(--vp-c-text-1);
  display: grid;
  gap: 22px;
  grid-template-columns: auto 1fr auto;
  overflow: hidden;
  padding: 24px 26px;
  position: relative;
  text-decoration: none !important;
  transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
}

.featured-card::before {
  background: linear-gradient(
    90deg,
    color-mix(in srgb, var(--nokv-accent) 60%, transparent),
    color-mix(in srgb, var(--nokv-accent-2) 60%, transparent) 60%,
    transparent
  );
  content: '';
  height: 2px;
  inset: 0 0 auto 0;
  opacity: 0.55;
  position: absolute;
}

.featured-card--dbdb::before {
  background: linear-gradient(
    90deg,
    color-mix(in srgb, var(--nokv-accent) 60%, transparent),
    color-mix(in srgb, var(--nokv-accent-3) 60%, transparent) 60%,
    transparent
  );
}

.featured-card:hover {
  border-color: color-mix(in srgb, var(--nokv-accent) 45%, transparent);
  box-shadow:
    0 24px 60px -20px color-mix(in srgb, var(--nokv-accent) 32%, transparent),
    var(--nokv-card-shadow);
  transform: translateY(-3px);
}

.featured-mark {
  align-items: center;
  background: color-mix(in srgb, var(--vp-c-bg-soft) 70%, transparent);
  border: 1px solid var(--vp-c-divider);
  border-radius: 14px;
  display: inline-flex;
  height: 64px;
  justify-content: center;
  width: 64px;
}

.dark .featured-mark {
  background: rgba(255, 255, 255, 0.04);
}

.featured-mark img {
  display: block;
  height: 40px;
  object-fit: contain;
  width: 40px;
}

.featured-meta {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}

.featured-eyebrow {
  color: var(--nokv-accent);
  font-family: var(--vp-font-family-mono);
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.featured-card--dbdb .featured-eyebrow {
  color: var(--nokv-accent-3);
}

.featured-meta strong {
  color: var(--vp-c-text-1);
  font-size: 1.18rem;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.featured-detail {
  color: var(--vp-c-text-3);
  font-size: 0.9rem;
}

.featured-arrow {
  align-items: center;
  background: color-mix(in srgb, var(--vp-c-bg-soft) 60%, transparent);
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  color: var(--vp-c-text-2);
  display: inline-flex;
  height: 44px;
  justify-content: center;
  transition: background 0.18s ease, color 0.18s ease, transform 0.18s ease;
  width: 44px;
}

.featured-card:hover .featured-arrow {
  background: color-mix(in srgb, var(--nokv-accent) 12%, transparent);
  color: var(--nokv-accent);
  transform: translate(2px, -2px);
}

/* Chip section — centered label + equal-width grid */
.chip-section {
  border-top: 1px dashed var(--vp-c-divider);
  padding-top: 24px;
  text-align: center;
}

.chip-label {
  color: var(--vp-c-text-3);
  display: block;
  font-family: var(--vp-font-family-mono);
  font-size: 0.74rem;
  font-weight: 600;
  letter-spacing: 0.14em;
  margin-bottom: 14px;
  text-transform: uppercase;
}

.chip-row {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.chip {
  align-items: center;
  background: var(--nokv-card-bg);
  border: 1px solid var(--vp-c-border);
  border-radius: 12px;
  color: var(--vp-c-text-1);
  display: flex;
  font-size: 0.92rem;
  gap: 10px;
  justify-content: flex-start;
  padding: 12px 16px;
  text-align: left;
  text-decoration: none !important;
  transition: border-color 0.18s ease, transform 0.18s ease;
}

.chip:hover {
  border-color: color-mix(in srgb, var(--nokv-accent) 50%, transparent);
  transform: translateY(-1px);
}

.chip strong {
  flex: 0 0 auto;
  font-weight: 700;
}

.chip span {
  color: var(--vp-c-text-3);
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.chip svg {
  color: var(--vp-c-text-3);
  flex: 0 0 auto;
}

.chip:hover svg {
  color: var(--nokv-accent);
}

@media (max-width: 900px) {
  .featured-row {
    grid-template-columns: 1fr;
  }

  .featured-card {
    grid-template-columns: auto 1fr;
  }

  .featured-arrow {
    display: none;
  }

  .chip-row {
    grid-template-columns: 1fr;
  }
}
</style>
