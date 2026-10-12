// In a section nav that scrolls sideways (narrow screens), show the current section.
(function () {
    const current = document.querySelector('.ishikari-admin [aria-current="page"]');
    const row = current && current.closest("ul");
    if (!row) return;
    const r = current.getBoundingClientRect(), box = row.getBoundingClientRect();
    if (r.right > box.right) row.scrollLeft += r.left - box.left - 8;
})();

// Tooltip for [data-tip] elements (e.g. full worker paths). Fixed-position so it isn't
// clipped by scrolling table containers. Runs once per page, however often it's included.
(function () {
    if (window.__ishikariTips) return;
    window.__ishikariTips = true;
    const tip = document.createElement("div");
    tip.className = "pointer-events-none fixed z-50 hidden max-w-lg rounded-field bg-neutral px-2 py-1 font-mono text-xs text-neutral-content shadow-lg break-all";
    tip.setAttribute("role", "tooltip");
    document.body.appendChild(tip);
    function hide() { tip.classList.add("hidden"); }
    document.addEventListener("mouseover", function (e) {
        const el = e.target.closest("[data-tip]");
        if (!el) return hide();
        tip.textContent = el.dataset.tip;
        tip.classList.remove("hidden");
        const r = el.getBoundingClientRect(), t = tip.getBoundingClientRect();
        const left = Math.min(Math.max(8, r.left), window.innerWidth - t.width - 8);
        const top = r.top - t.height - 6 < 8 ? r.bottom + 6 : r.top - t.height - 6;
        tip.style.left = left + "px";
        tip.style.top = top + "px";
    });
    window.addEventListener("scroll", hide, true);
})();
