<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<script setup lang="ts">
import { withBase } from 'vitepress'
</script>

<template>
  <section class="cta-wrap nokv-section">
    <div class="cta nokv-card">
      <div class="nokv-glow" aria-hidden="true" />

      <div class="cta-copy">
        <span class="nokv-eyebrow">Looking for design partners</span>
        <h2 class="nokv-h2">
          Built from scratch. Apache-2.0.
          <span class="nokv-gradient-text">Open to collaboration.</span>
        </h2>
        <p class="nokv-lead">
          Working on distributed filesystems, object storage namespaces, AI dataset
          infrastructure, or agent workspace metadata? We are looking for
          contributors and design partners. Drop us a line, or jump straight into
          the docs.
        </p>

        <div class="cta-row">
          <a class="nokv-btn nokv-btn--primary" :href="withBase('/guide/')">
            Read the docs
            <span class="arrow" aria-hidden="true">→</span>
          </a>
          <a
            class="nokv-btn nokv-btn--ghost"
            href="https://github.com/feichai0017/NoKV"
            target="_blank"
            rel="noreferrer"
          >
            View on GitHub
          </a>
          <a class="nokv-btn nokv-btn--ghost" href="mailto:contact@nokv.io">
            Contact the team
          </a>
        </div>
      </div>

      <aside class="cta-side">
        <span class="cta-side-label">Maintainers</span>
        <ul class="cta-side-list">
          <li>
            <strong>Eric</strong>
            <a href="mailto:eric@nokv.io">eric@nokv.io</a>
          </li>
          <li>
            <strong>Jason</strong>
            <a href="mailto:jason@nokv.io">jason@nokv.io</a>
          </li>
        </ul>
      </aside>
    </div>
  </section>
</template>

<style scoped>
.cta-wrap {
  padding-top: 32px;
}

.cta {
  background: linear-gradient(
    135deg,
    color-mix(in srgb, var(--nokv-accent) 12%, var(--vp-c-bg-soft)) 0%,
    var(--vp-c-bg-soft) 100%
  );
  border: 1px solid color-mix(in srgb, var(--nokv-accent) 24%, var(--vp-c-border));
  display: grid;
  gap: 36px;
  grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
  overflow: hidden;
  padding: 48px 48px 44px;
  position: relative;
}

.cta .nokv-glow {
  inset: -50% -10% auto auto;
  height: 320px;
  width: 320px;
}

.cta-copy {
  position: relative;
  z-index: 1;
}

.cta-copy h2 {
  font-size: clamp(1.7rem, 3vw, 2.2rem);
  margin-bottom: 16px;
}

.cta-row {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 28px;
}

.cta-side {
  align-self: end;
  background: color-mix(in srgb, var(--vp-c-bg) 50%, transparent);
  border: 1px solid var(--vp-c-divider);
  border-radius: 14px;
  padding: 20px 22px;
  position: relative;
  z-index: 1;
}

.cta-side-label {
  color: var(--vp-c-text-3);
  display: block;
  font-family: var(--vp-font-family-mono);
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.12em;
  margin-bottom: 14px;
  text-transform: uppercase;
}

.cta-side-list {
  display: grid;
  gap: 10px;
  list-style: none;
  margin: 0;
  padding: 0;
}

.cta-side-list li {
  display: flex;
  flex-direction: column;
}

.cta-side-list strong {
  color: var(--vp-c-text-1);
  font-size: 0.95rem;
}

.cta-side-list a {
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 0.85rem;
  text-decoration: none;
}

.cta-side-list a:hover {
  color: var(--nokv-accent);
}

@media (max-width: 900px) {
  .cta {
    grid-template-columns: 1fr;
    padding: 36px 28px;
  }
}
</style>
