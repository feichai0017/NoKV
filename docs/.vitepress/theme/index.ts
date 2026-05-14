// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'

import './style.css'

import HomeHero from './components/HomeHero.vue'
import HomeRecognition from './components/HomeRecognition.vue'
import HomeStats from './components/HomeStats.vue'
import HomeAudiences from './components/HomeAudiences.vue'
import HomeWhy from './components/HomeWhy.vue'
import HomePrimitives from './components/HomePrimitives.vue'
import HomeArch from './components/HomeArch.vue'
import HomeBench from './components/HomeBench.vue'
import HomeCompare from './components/HomeCompare.vue'
import HomeCTA from './components/HomeCTA.vue'
import SiteFooter from './components/SiteFooter.vue'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('HomeHero', HomeHero)
    app.component('HomeRecognition', HomeRecognition)
    app.component('HomeStats', HomeStats)
    app.component('HomeAudiences', HomeAudiences)
    app.component('HomeWhy', HomeWhy)
    app.component('HomePrimitives', HomePrimitives)
    app.component('HomeArch', HomeArch)
    app.component('HomeBench', HomeBench)
    app.component('HomeCompare', HomeCompare)
    app.component('HomeCTA', HomeCTA)
    app.component('SiteFooter', SiteFooter)
  },
} satisfies Theme
