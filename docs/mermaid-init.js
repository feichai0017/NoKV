(() => {
  const script = document.createElement("script");
  script.src = "https://cdn.jsdelivr.net/npm/mermaid@10.9.0/dist/mermaid.min.js";
  script.onload = () => {
    mermaid.initialize({ startOnLoad: true });
  };
  document.head.appendChild(script);
})();
