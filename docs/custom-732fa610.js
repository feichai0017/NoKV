(() => {
  const root = document.documentElement;
  root.classList.add("has-js");

  const pathname = window.location.pathname;
  if (pathname.endsWith("/index.html") || pathname.endsWith("/NoKV/") || pathname.endsWith("/NoKV")) {
    root.classList.add("is-overview-page");
  }

  const main = document.querySelector(".content main");
  if (!main) {
    return;
  }

  const progressRail = document.createElement("div");
  progressRail.className = "progress-rail";
  progressRail.innerHTML = '<div class="progress-bar"></div>';
  document.body.appendChild(progressRail);
  const progressBar = progressRail.querySelector(".progress-bar");

  const updateProgress = () => {
    const scrollTop = window.scrollY || document.documentElement.scrollTop;
    const height = document.documentElement.scrollHeight - window.innerHeight;
    const ratio = height > 0 ? Math.min(scrollTop / height, 1) : 0;
    progressBar.style.width = `${ratio * 100}%`;
  };
  updateProgress();
  window.addEventListener("scroll", updateProgress, { passive: true });

  const firstParagraph = Array.from(main.children).find((node) => node.tagName === "P");
  if (firstParagraph && !firstParagraph.closest(".hero")) {
    firstParagraph.classList.add("chapter-lead");
  }

  main.querySelectorAll("table").forEach((table) => {
    if (table.parentElement && table.parentElement.classList.contains("table-wrap")) {
      return;
    }
    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    table.parentNode.insertBefore(wrap, table);
    wrap.appendChild(table);
  });

  const decorateHeading = (heading) => {
    if (!heading.id) {
      return;
    }
    if (heading.querySelector(".heading-anchor")) {
      return;
    }
    const anchor = document.createElement("a");
    anchor.className = "heading-anchor";
    anchor.href = `#${heading.id}`;
    anchor.setAttribute("aria-label", `Link to ${heading.textContent}`);
    anchor.textContent = "#";
    heading.appendChild(anchor);
  };

  const sectionHeadings = Array.from(main.querySelectorAll("h2"));
  sectionHeadings.forEach(decorateHeading);
  Array.from(main.querySelectorAll("h3")).forEach(decorateHeading);

  if (!root.classList.contains("is-overview-page") && sectionHeadings.length >= 2) {
    const toc = document.createElement("nav");
    toc.className = "mini-toc";
    toc.innerHTML = '<div class="mini-toc-title">On this page</div><ul class="mini-toc-list"></ul>';
    const tocList = toc.querySelector(".mini-toc-list");
    sectionHeadings.forEach((heading) => {
      if (!heading.id) {
        return;
      }
      const item = document.createElement("li");
      const link = document.createElement("a");
      link.href = `#${heading.id}`;
      link.textContent = heading.textContent.replace(/#$/, "").trim();
      item.appendChild(link);
      tocList.appendChild(item);
    });
    const insertAfter = firstParagraph || main.querySelector("h1");
    if (insertAfter && insertAfter.parentNode === main) {
      insertAfter.insertAdjacentElement("afterend", toc);
    } else {
      main.prepend(toc);
    }

    const tocLinks = Array.from(toc.querySelectorAll("a"));
    const updateActiveHeading = () => {
      let activeId = "";
      for (const heading of sectionHeadings) {
        const rect = heading.getBoundingClientRect();
        if (rect.top <= 120) {
          activeId = heading.id;
        }
      }
      tocLinks.forEach((link) => {
        link.classList.toggle("is-active", link.getAttribute("href") === `#${activeId}`);
      });
    };
    updateActiveHeading();
    window.addEventListener("scroll", updateActiveHeading, { passive: true });
  }

  const revealTargets = Array.from(
    main.querySelectorAll(
      ".hero, .feature-card, .doc-card, .quicklink, .benchmark-note, .mini-toc, blockquote, pre, .table-wrap, h2, h3, .chapter-lead"
    )
  );
  revealTargets.forEach((node) => node.setAttribute("data-reveal", ""));

  if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            observer.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.12, rootMargin: "0px 0px -24px 0px" }
    );
    revealTargets.forEach((node) => observer.observe(node));
  } else {
    revealTargets.forEach((node) => node.classList.add("is-visible"));
  }

  const backToTop = document.createElement("button");
  backToTop.className = "back-to-top";
  backToTop.type = "button";
  backToTop.innerHTML = '<span>↑</span><span>Top</span>';
  backToTop.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });
  document.body.appendChild(backToTop);

  const toggleBackToTop = () => {
    backToTop.classList.toggle("is-visible", (window.scrollY || document.documentElement.scrollTop) > 360);
  };
  toggleBackToTop();
  window.addEventListener("scroll", toggleBackToTop, { passive: true });
})();
