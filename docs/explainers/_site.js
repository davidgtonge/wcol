(() => {
  "use strict";

  const CHAPTERS = Object.freeze([
    {
      id: "01",
      href: "01-sql-to-graph.html",
      short: "SQL",
      nav: "01 SQL",
      title: "From SQL to a distributed graph",
      blurb: "A query becomes a streaming operator graph that can run across memory-constrained nodes.",
    },
    {
      id: "02",
      href: "02-exchanges.html",
      short: "Exchanges",
      nav: "02 Exchanges",
      title: "Where rows go changes the query",
      blurb: "Hash, range, and gather routing decide which rows meet and which answers stay correct.",
    },
    {
      id: "03",
      href: "03-parquet.html",
      short: "Parquet",
      nav: "03 Parquet",
      title: "From Thrift to decoded rows",
      blurb: "Walk Parquet metadata without object trees, then decode pages into typed values.",
    },
    {
      id: "04",
      href: "04-streaming.html",
      short: "Streaming",
      nav: "04 Streaming",
      title: "A query in motion",
      blurb: "Bounded mailboxes, RPC streams, and backpressure keep the host from buffering forever.",
    },
  ]);

  const SOURCE = "https://github.com/davidgtonge/wcol";

  function esc(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function currentChapterId() {
    return document.body?.dataset?.chapter || "";
  }

  function findChapter(id) {
    return CHAPTERS.find((chapter) => chapter.id === id) || null;
  }

  function chapterIndex(id) {
    return CHAPTERS.findIndex((chapter) => chapter.id === id);
  }

  function injectHeader() {
    if (document.querySelector(".wcol-site-header")) return;

    const active = currentChapterId();
    const header = document.createElement("header");
    header.className = "wcol-site-header";
    header.innerHTML = `
      <div class="wcol-site-header-inner">
        <a class="wcol-site-brand" href="index.html" aria-label="WCOL systems explainers home">
          <span class="wcol-site-brand-mark" aria-hidden="true"><i></i><i></i><i></i><i></i></span>
          <span class="wcol-site-brand-word">wcol<span>/</span></span>
          <span class="wcol-site-brand-divider" aria-hidden="true"></span>
          <span class="wcol-site-brand-label">SYSTEMS EXPLAINERS</span>
        </a>
        <nav class="wcol-site-nav" aria-label="Explainer chapters">
          <div class="wcol-site-chapters">
            ${CHAPTERS.map(
              (chapter) =>
                `<a href="${esc(chapter.href)}" class="${chapter.id === active ? "is-active" : ""}" ${
                  chapter.id === active ? 'aria-current="page"' : ""
                }>${esc(chapter.id)} <span>${esc(chapter.short)}</span></a>`
            ).join("")}
          </div>
          <a class="wcol-site-source" href="${SOURCE}" target="_blank" rel="noreferrer noopener">SOURCE ↗</a>
        </nav>
      </div>
    `;

    document.body.insertBefore(header, document.body.firstChild);
  }

  function injectPager() {
    if (document.querySelector(".wcol-site-pager")) return;

    const active = currentChapterId();
    const index = chapterIndex(active);
    if (index < 0) return;

    const prev = CHAPTERS[index - 1] || null;
    const next = CHAPTERS[index + 1] || null;
    const pager = document.createElement("nav");
    pager.className = "wcol-site-pager";
    pager.setAttribute("aria-label", "Chapter navigation");
    pager.innerHTML = `
      <div class="wcol-site-pager-inner">
        ${
          prev
            ? `<a href="${esc(prev.href)}">
                <span class="wcol-site-pager-kicker">← PREV · ${esc(prev.id)}</span>
                <span class="wcol-site-pager-title">${esc(prev.title)}</span>
                <span class="wcol-site-pager-blurb">${esc(prev.blurb)}</span>
              </a>`
            : `<a class="is-empty" aria-hidden="true" tabindex="-1"></a>`
        }
        ${
          next
            ? `<a class="wcol-site-pager-next" href="${esc(next.href)}">
                <span class="wcol-site-pager-kicker">NEXT · ${esc(next.id)} →</span>
                <span class="wcol-site-pager-title">${esc(next.title)}</span>
                <span class="wcol-site-pager-blurb">${esc(next.blurb)}</span>
              </a>`
            : `<a class="is-empty" aria-hidden="true" tabindex="-1"></a>`
        }
      </div>
    `;

    const footer = document.querySelector("footer");
    if (footer) {
      footer.parentNode.insertBefore(pager, footer);
    } else {
      document.body.appendChild(pager);
    }
  }

  function renderHubCards(target) {
    if (!target) return;
    target.innerHTML = CHAPTERS.map(
      (chapter) => `
      <a class="hub-card" href="${esc(chapter.href)}">
        <span class="hub-card-index">${esc(chapter.id)}</span>
        <strong>${esc(chapter.title)}</strong>
        <p>${esc(chapter.blurb)}</p>
        <span class="hub-card-cta">OPEN CHAPTER <span aria-hidden="true">↗</span></span>
      </a>`
    ).join("");
  }

  function init() {
    injectHeader();
    injectPager();
    renderHubCards(document.getElementById("hub-chapters"));
  }

  window.WCOL_EXPLAINERS = Object.freeze({
    chapters: CHAPTERS,
    findChapter,
    renderHubCards,
  });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
