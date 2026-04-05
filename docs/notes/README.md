# 设计笔记与实现记录

这一节是 NoKV 的长文档区域。

主文档更偏参考手册，`notes` 更像工程记录与技术随笔，主要用来解释：

- 某个边界为什么存在
- 某次实现为什么要推翻重做
- 哪些“看起来简单”的方案最后证明是错的
- 代码库在演进过程中学到了什么

<div class="blog-hero">
  <div class="blog-hero-copy">
    <span class="masthead-kicker">工程日志</span>
    <h2>NoKV 是怎么被做出来的</h2>
    <p>这些文章记录设计取舍、实现教训、调试过程，以及代码中那些边界背后的原因。</p>
  </div>
  <div class="blog-hero-meta">
    <div class="tag-pill">设计</div>
    <div class="tag-pill">实现</div>
    <div class="tag-pill">分布式系统</div>
    <div class="tag-pill">存储内核</div>
  </div>
</div>

## 推荐阅读

<div class="blog-grid">
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-standalone-to-distributed-bridge.html">standalone 到 distributed 的桥接</a></h3>
    <p>为什么 NoKV 把单机和分布式视为同一套系统，以及为什么迁移必须做成协议而不是 dump/import 工具。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-pd-and-raftadmin-layering.html">PD 与 RaftAdmin 分层</a></h3>
    <p>为什么 control plane 和 execution plane 必须分开，以及为什么 PD 不能直接写本地 truth。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-migration-mode-and-snapshot.html">migration 的 mode 与 snapshot</a></h3>
    <p>迁移的本体其实是生命周期和快照语义，而不是补几条 CLI 命令。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-distributed-testing-and-failpoints.html">分布式测试与 failpoint</a></h3>
    <p>为什么 NoKV 同时使用 live integration 与窄边界 failpoint，以及 failpoint 应该如何克制。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-03</span>
    <h3><a href="2026-04-03-delos-lite-metadata-root-roadmap.html">Rooted Metadata、Delos-lite 与 Virtual Log</a></h3>
    <p>完整说明 NoKV 当前的 metadata truth、PD 隔离、Virtual Log substrate、local/replicated backend，以及为什么这条主线适合作为后续研究平台。</p>
  </div>
</div>

## 这里应该写什么

- 参考文档放不下的设计取舍
- 有明确症状、错误假设和最终修复的调试记录
- 带 benchmark 背景和代码路径分析的性能调查
- 解释包边界为何变化的重构说明

## 写作风格

每篇 note 都应该像一篇小型技术博客，但保持工程视角：

1. 从具体问题或设计问题开始
2. 先把系统边界讲清楚
3. 讲清 tradeoff 和被否决的方案
4. 需要时放图、代码路径和命令
5. 最后说明代码里已经改了什么、还有什么没解决

## 建议模板

```md
# 标题

## 为什么这件事重要

## 当前系统边界

## 看起来简单但其实错的路

## 我们最终采用的设计

## 关键代码路径

## 图示

## 这次改变了什么

## 还没解决什么
```

## 新增一篇 note

1. 创建 `docs/notes/YYYY-MM-DD-short-title.md`
2. 把它加到 `docs/SUMMARY.md`
3. 优先写清楚图、代码路径和明确文件引用，少写空泛描述
