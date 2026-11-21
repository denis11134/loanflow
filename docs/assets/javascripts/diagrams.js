/* Только оформление готового SVG. Исходное содержание Mermaid не меняется. */
(() => {
  const content = document.querySelector(".md-content");
  if (!content) return;
  const watched = new WeakSet();
  const resize = new ResizeObserver(() => update());
  function update() {
    content.querySelectorAll("div.mermaid").forEach(diagram => {
      diagram.tabIndex = 0;
      diagram.setAttribute("role", "region");
      diagram.setAttribute("aria-label", "Диаграмма с горизонтальной прокруткой");
      if (!watched.has(diagram)) {
        watched.add(diagram);
        resize.observe(diagram);
      }
      const wide = diagram.scrollWidth > diagram.clientWidth + 1;
      let help = diagram.previousElementSibling;
      if (!help?.classList.contains("diagram-help") && wide) {
        help = document.createElement("p");
        help.className = "diagram-help";
        help.textContent = "Схему можно прокручивать по горизонтали, в том числе клавишами ← и →.";
        diagram.before(help);
      }
      if (help?.classList.contains("diagram-help")) help.hidden = !wide;
    });
  }
  new MutationObserver(update).observe(content, {childList: true, subtree: true});
  update();
})();
