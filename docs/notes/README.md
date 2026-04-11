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
    <h3><a href="2026-03-30-standalone-to-distributed-bridge.html">从单机到分布式的桥接</a></h3>
    <p>解释为什么 NoKV 把单机和分布式视为同一套系统，以及为什么迁移必须做成协议而不是 dump/import 工具。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-coordinator-and-execution-layering.html">Coordinator 与执行面分层</a></h3>
    <p>解释为什么 control plane、truth kernel 和 data-plane executor 必须分开，以及为什么 Coordinator 不能直接写本地 truth。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-migration-mode-and-snapshot.html">迁移里的 mode 与 snapshot 语义</a></h3>
    <p>说明 migration 的本体其实是目录生命周期协议和 snapshot 分层，而不是补几条 CLI 命令。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-distributed-testing-and-failpoints.html">分布式测试与 failpoint</a></h3>
    <p>解释为什么 NoKV 同时使用 live integration、testcluster 和窄边界 failpoint，以及 failpoint 应该如何克制。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-31</span>
    <h3><a href="2026-03-31-sst-snapshot-install.html">基于 SST 的 Snapshot Install</a></h3>
    <p>说明 NoKV 为什么选择 region-scoped、self-contained、与源端 vlog 独立的 SST snapshot install 方案。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-03</span>
    <h3><a href="2026-04-03-delos-lite-metadata-root-roadmap.html">Rooted Metadata、Delos-lite 与 VirtualLog</a></h3>
    <p>完整说明 NoKV 当前的 metadata truth、Coordinator 隔离、VirtualLog contract、local/replicated backend，以及为什么这条主线适合作为后续研究平台。</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-05</span>
    <h3><a href="2026-04-05-range-filter-from-grf.html">Range Filter：从 GRF 得到启发，但不照搬 GRF</a></h3>
    <p>解释 NoKV 为什么需要 read-path pruning、GRF 到底提供了什么启发、为什么当前实现选择更保守的 in-memory advisory 方案，以及它与 LSM 读路径的关系。</p>
  </div>
</div>

## 这里应该写什么

- 参考文档放不下的设计取舍
- 有明确症状、错误假设和最终修复的调试记录
- 带 benchmark 背景和设计解释的性能调查
- 解释包边界为何变化的重构说明

## 写作风格

每篇 note 都应该像一篇小型技术博客，但保持工程视角：

1. 从具体问题或设计问题开始
2. 先把系统边界讲清楚
3. 讲清 tradeoff 和被否决的方案
4. 需要时放图、调用链、对象关系和命令
5. 最后说明代码里已经改了什么、还有什么没解决

`notes` 的重点是解释设计和演进，而不是把正文写成文件导航。除非某个模块名或文件名对理解确实必要，否则不需要强制罗列具体代码路径；相比“这个函数在哪”，更重要的是“为什么这样分层、为什么不选别的路、边界到底是什么”。

建议在最近的设计类 note 顶部增加一个简短导读块，至少包含：

- 🧭 主题
- 🧱 核心对象
- 🔁 调用链
- 📚 参考对象

## 建议模板

```md
# 标题

## 导读

- 🧭 主题：
- 🧱 核心对象：
- 🔁 调用链：
- 📚 参考对象：

## 为什么这件事重要

## 当前系统边界

## 看起来简单但其实错的路

## 我们最终采用的设计

## 关键对象与边界

## 图示与调用逻辑

## 设计理念

## 参考对象

## 这次改变了什么

## 还没解决什么
```

## 新增一篇 note

1. 创建 `docs/notes/YYYY-MM-DD-short-title.md`
2. 把它加到 `docs/SUMMARY.md`
3. 优先写清楚图、对象关系、调用链和设计理念，少写空泛描述
4. `notes` 统一使用中文撰写；必要时保留英文术语，但不要把正文写成中英混杂的半成品
