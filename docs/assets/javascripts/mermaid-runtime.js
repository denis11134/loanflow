/* Material повторно вызывает initialize. Сохраняем естественный размер схем
   при каждом вызове, чтобы подписи не уменьшались до нечитаемого масштаба. */
(() => {
  const initialize = mermaid.initialize.bind(mermaid);
  mermaid.initialize = (options = {}) => initialize({
    ...options,
    startOnLoad: false,
    flowchart: {...options.flowchart, useMaxWidth: false},
    sequence: {...options.sequence, useMaxWidth: false},
    er: {...options.er, useMaxWidth: false},
    c4: {...options.c4, useMaxWidth: false}
  });
  mermaid.initialize();
})();
