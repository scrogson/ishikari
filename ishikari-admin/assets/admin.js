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
    tip.className = "ik-tooltip";
    tip.hidden = true;
    tip.setAttribute("role", "tooltip");
    document.body.appendChild(tip);
    function hide() { tip.hidden = true; }
    document.addEventListener("mouseover", function (e) {
        const el = e.target.closest("[data-tip]");
        if (!el) return hide();
        tip.textContent = el.dataset.tip;
        tip.hidden = false;
        const r = el.getBoundingClientRect(), t = tip.getBoundingClientRect();
        const left = Math.min(Math.max(8, r.left), window.innerWidth - t.width - 8);
        const top = r.top - t.height - 6 < 8 ? r.bottom + 6 : r.top - t.height - 6;
        tip.style.left = left + "px";
        tip.style.top = top + "px";
    });
    window.addEventListener("scroll", hide, true);
})();

// Graphs ([data-ik-graph]): lays the nodes ([data-node], naming their upstreams in data-deps)
// out left to right in layers, draws the edges, and makes the canvas pannable (drag, sideways
// scroll) and zoomable (ctrl/⌘ + scroll, pinch, the toolbar). An edge takes its look from its
// upstream's data-state. Hovering or focusing a node traces what it depends on and what
// depends on it.
(function () {
    const GAP_X = 88, GAP_Y = 18, LANE = 10, MIN_H = 200, MAX_H = 560;
    const INSET = { x: 24, top: 52, bottom: 24, legend: 48 };
    const MIN_K = 0.2, MAX_K = 2, FIT_K = 0.6; // fitting never shrinks text below FIT_K
    const NS = "http://www.w3.org/2000/svg";
    const ICONS = {
        out: '<path d="M5 12h14"/>',
        in: '<path d="M5 12h14"/><path d="M12 5v14"/>',
        fit: '<path d="M8 3H5a2 2 0 0 0-2 2v3"/><path d="M21 8V5a2 2 0 0 0-2-2h-3"/><path d="M3 16v3a2 2 0 0 0 2 2h3"/><path d="M16 21h3a2 2 0 0 0 2-2v-3"/>',
    };
    const KINDS = { done: "completed", active: "running", wait: "waiting", failed: "failed" };
    const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

    function edgeKind(state) {
        switch (state) {
            case undefined: case "": return "plain";
            case "completed": case "satisfied": return "done";
            case "discarded": case "cancelled": case "failed": return "failed";
            case "executing": case "running": return "active";
            default: return "wait";
        }
    }

    // Layered layout: longest-path layers, a dummy vertex wherever an edge crosses a layer,
    // barycenter ordering, then each layer's y positions nudged toward their neighbors.
    function layout(nodes) {
        const layer = new Map(), visiting = new Set();
        const depth = (id) => {
            if (layer.has(id)) return layer.get(id);
            if (visiting.has(id)) return 0; // a cycle: cut it here
            visiting.add(id);
            let d = 0;
            for (const up of nodes.get(id).deps) d = Math.max(d, depth(up) + 1);
            visiting.delete(id);
            layer.set(id, d);
            return d;
        };
        nodes.forEach((_, id) => depth(id));
        // A source sits just before its first dependent rather than at the far left.
        nodes.forEach((n, id) => {
            if (n.deps.length) return;
            let min = Infinity;
            nodes.forEach((m, mid) => { if (m.deps.includes(id)) min = Math.min(min, layer.get(mid)); });
            if (min !== Infinity) layer.set(id, min - 1);
        });

        const V = new Map(), edges = [];
        nodes.forEach((n, id) => V.set(id, { id, real: true, layer: layer.get(id), w: n.w, h: n.h, ups: [], downs: [] }));
        const link = (a, b) => { V.get(a).downs.push(b); V.get(b).ups.push(a); };
        let k = 0;
        nodes.forEach((n, id) => {
            for (const up of n.deps) {
                const path = [up];
                if (layer.get(up) < layer.get(id)) {
                    let prev = up;
                    for (let l = layer.get(up) + 1; l < layer.get(id); l++) {
                        const d = "\u0000" + k++;
                        V.set(d, { id: d, layer: l, w: 0, h: 0, ups: [], downs: [] });
                        link(prev, d);
                        path.push(d);
                        prev = d;
                    }
                    link(prev, id);
                }
                path.push(id);
                edges.push({ from: up, to: id, path });
            }
        });

        const rows = [];
        V.forEach((v) => (rows[v.layer] = rows[v.layer] || []).push(v));
        for (let i = 0; i < rows.length; i++) rows[i] = rows[i] || [];
        rows.forEach((row) => row.forEach((v, i) => (v.order = i)));
        const mean = (v, side, key, own) => {
            const ns = side === "both" ? v.ups.concat(v.downs) : v[side];
            return ns.length ? ns.reduce((s, n) => s + V.get(n)[key], 0) / ns.length : own;
        };
        const sweeps = (n, each) => {
            for (let it = 0; it < n; it++) {
                const down = it % 2 === 0;
                const seq = down ? rows.slice(1) : rows.slice(0, -1).reverse();
                seq.forEach((row) => each(row, down ? "ups" : "downs"));
            }
        };
        sweeps(12, (row, side) => {
            row.forEach((v) => (v.b = mean(v, side, "order", v.order)));
            row.sort((a, b) => a.b - b.b || a.order - b.order);
            row.forEach((v, i) => (v.order = i));
        });

        const colX = [], colW = [];
        let x = 0;
        rows.forEach((row, l) => {
            const w = Math.max(0, ...row.map((v) => v.w));
            colX[l] = x;
            colW[l] = w;
            row.forEach((v) => (v.x = x + (w - v.w) / 2));
            x += w + GAP_X;
        });

        // Place a layer's centers as near the wanted ones as their order and spacing allow
        // (isotonic regression by pooling adjacent violators).
        const pack = (row, want) => {
            let o = 0;
            const blocks = [];
            row.forEach((v, i) => {
                if (i) o += (row[i - 1].h + v.h) / 2 + (row[i - 1].real && v.real ? GAP_Y : LANE);
                v.o = o;
                blocks.push({ sum: want[i] - o, n: 1 });
                while (blocks.length > 1) {
                    const b = blocks[blocks.length - 1], a = blocks[blocks.length - 2];
                    if (a.sum / a.n <= b.sum / b.n) break;
                    a.sum += b.sum;
                    a.n += b.n;
                    blocks.pop();
                }
            });
            let i = 0;
            blocks.forEach((b) => { for (let j = 0; j < b.n; j++, i++) row[i].y = row[i].o + b.sum / b.n; });
        };
        rows.forEach((row) => pack(row, row.map(() => 0)));
        sweeps(8, (row, side) => pack(row, row.map((v) => mean(v, side, "y", v.y))));
        rows.forEach((row) => pack(row, row.map((v) => mean(v, "both", "y", v.y))));

        let top = Infinity, bottom = -Infinity;
        V.forEach((v) => { top = Math.min(top, v.y - v.h / 2); bottom = Math.max(bottom, v.y + v.h / 2); });
        V.forEach((v) => (v.y -= top));

        // Edge points: out of the upstream's right side, along each dummy's lane, into the
        // downstream's left side.
        edges.forEach((e) => {
            const pts = [];
            e.path.forEach((id, i) => {
                const v = V.get(id);
                if (i === 0) pts.push([v.x + v.w, v.y]);
                else if (!v.real) pts.push([colX[v.layer], v.y], [colX[v.layer] + colW[v.layer], v.y]);
                else pts.push([v.x, v.y]);
            });
            e.pts = pts;
        });
        return { V, edges, width: Math.max(0, x - GAP_X), height: bottom - top };
    }

    function curve(pts) {
        const end = pts[pts.length - 1];
        let d = "M" + pts[0][0] + " " + pts[0][1];
        for (let i = 1; i < pts.length; i++) {
            const [ax, ay] = pts[i - 1];
            let [bx, by] = pts[i];
            if (i === pts.length - 1) bx -= 5; // leave room for the arrowhead
            if (ay === by && bx > ax) { d += " L" + bx + " " + by; continue; }
            const c = Math.max(Math.abs(bx - ax) / 2, 40);
            d += " C" + (ax + c) + " " + ay + " " + (bx - c) + " " + by + " " + bx + " " + by;
        }
        const [tx, ty] = end;
        return [d, "M" + (tx - 7) + " " + (ty - 4) + " L" + tx + " " + ty + " L" + (tx - 7) + " " + (ty + 4) + " Z"];
    }

    function svg(tag, attrs) {
        const el = document.createElementNS(NS, tag);
        for (const k in attrs) el.setAttribute(k, attrs[k]);
        return el;
    }

    function init(root) {
        if (root.__ikGraph) return;
        const viewport = root.querySelector(".ik-graph-viewport");
        const world = root.querySelector(".ik-graph-world");
        const els = viewport && world ? Array.from(world.querySelectorAll("[data-node]")) : [];
        if (!els.length) return;
        root.__ikGraph = true;

        const byId = new Map(els.map((el) => [el.dataset.node, el]));
        const deps = new Map(), downs = new Map();
        els.forEach((el) => {
            const id = el.dataset.node;
            const ups = Array.from(new Set((el.dataset.deps || "").split(/\s+/)))
                .filter((d) => d && d !== id && byId.has(d));
            deps.set(id, ups);
            downs.set(id, downs.get(id) || []);
            ups.forEach((u) => (downs.get(u) || downs.set(u, []).get(u)).push(id));
        });

        const edgeLayer = svg("svg", { class: "ik-graph-edges", "aria-hidden": "true" });
        world.prepend(edgeLayer);

        const bar = document.createElement("div");
        bar.className = "ik-graph-toolbar";
        const button = (name, label) =>
            '<button type="button" data-zoom="' + name + '" aria-label="' + label + '" title="' + label + '">' +
            '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
            ICONS[name] + "</svg></button>";
        bar.innerHTML = button("out", "Zoom out") +
            '<output title="Ctrl/⌘ + scroll or pinch to zoom">100%</output>' +
            button("in", "Zoom in") + button("fit", "Fit to view");
        root.append(bar);
        const readout = bar.querySelector("output");

        const usedKinds = Object.keys(KINDS).filter((k) =>
            els.some((el) => (downs.get(el.dataset.node) || []).length && edgeKind(el.dataset.state) === k));
        if (usedKinds.length) {
            const legend = document.createElement("div");
            legend.className = "ik-graph-legend";
            legend.setAttribute("aria-hidden", "true");
            legend.innerHTML = usedKinds.map((k) =>
                '<span><svg class="ik-graph-edge is-' + k + '" viewBox="0 0 20 8"><path d="M0 4h20"/></svg>' + KINDS[k] + "</span>").join("");
            root.append(legend);
        }
        const inset = { x: INSET.x, top: INSET.top, bottom: usedKinds.length ? INSET.legend : INSET.bottom };

        root.classList.add("is-ready");
        const view = { x: 0, y: 0, k: 1, w: 0, h: 0, touched: false, built: false };
        const edgeEls = [];

        function apply() {
            world.style.transform = "translate(" + view.x + "px, " + view.y + "px) scale(" + view.k + ")";
            root.style.backgroundPosition = view.x + "px " + view.y + "px";
            root.style.backgroundSize = 20 * view.k + "px " + 20 * view.k + "px";
            readout.textContent = Math.round(view.k * 100) + "%";
        }

        function fit() {
            const vw = viewport.clientWidth;
            const maxH = Math.max(MIN_H, Math.min(MAX_H, window.innerHeight * 0.7));
            const k = clamp(Math.min(1, (vw - 2 * inset.x) / view.w, (maxH - inset.top - inset.bottom) / view.h), FIT_K, 1);
            const vh = clamp(view.h * k + inset.top + inset.bottom, MIN_H, maxH);
            viewport.style.height = vh + "px";
            view.k = k;
            // Centered when it fits, else from its start (the left, the top) to pan on from.
            const spareX = vw - 2 * inset.x - view.w * k, spareY = vh - inset.top - inset.bottom - view.h * k;
            view.x = inset.x + Math.max(0, spareX / 2);
            view.y = inset.top + Math.max(0, spareY / 2);
            view.touched = false;
            apply();
        }

        function build() {
            if (!viewport.clientWidth) return; // hidden: the resize observer builds it once shown
            const nodes = new Map();
            els.forEach((el) => nodes.set(el.dataset.node, { el, deps: deps.get(el.dataset.node), w: el.offsetWidth, h: el.offsetHeight }));
            const g = layout(nodes);
            g.V.forEach((v) => {
                if (!v.real) return;
                const el = nodes.get(v.id).el;
                el.style.left = v.x + "px";
                el.style.top = v.y - v.h / 2 + "px";
            });
            view.w = Math.max(g.width, 1);
            view.h = Math.max(g.height, 1);
            world.style.width = view.w + "px";
            world.style.height = view.h + "px";
            edgeLayer.setAttribute("width", view.w);
            edgeLayer.setAttribute("height", view.h);
            edgeLayer.replaceChildren();
            edgeEls.length = 0;
            const rank = { plain: 0, wait: 1, done: 2, failed: 3, active: 4 };
            g.edges
                .map((e) => Object.assign(e, { kind: edgeKind(byId.get(e.from).dataset.state) }))
                .sort((a, b) => rank[a.kind] - rank[b.kind])
                .forEach((e) => {
                    const [line, arrow] = curve(e.pts);
                    const el = svg("g", { class: "ik-graph-edge is-" + e.kind });
                    el.append(svg("path", { d: line }), svg("path", { d: arrow }));
                    edgeLayer.append(el);
                    edgeEls.push({ el, from: e.from, to: e.to });
                });
            view.built = true;
            fit();
        }

        function zoomAt(factor, cx, cy) {
            const k = clamp(view.k * factor, MIN_K, MAX_K);
            view.x = cx - (cx - view.x) * (k / view.k);
            view.y = cy - (cy - view.y) * (k / view.k);
            view.k = k;
            view.touched = true;
            apply();
        }

        bar.addEventListener("click", (e) => {
            const b = e.target.closest("button");
            if (!b) return;
            if (b.dataset.zoom === "fit") return fit();
            zoomAt(b.dataset.zoom === "in" ? 1.25 : 0.8, viewport.clientWidth / 2, viewport.clientHeight / 2);
        });

        viewport.addEventListener("wheel", (e) => {
            const r = viewport.getBoundingClientRect();
            if (e.ctrlKey || e.metaKey) {
                e.preventDefault();
                const delta = clamp(e.deltaMode ? e.deltaY * 33 : e.deltaY, -120, 120);
                zoomAt(Math.exp(-delta * 0.0025), e.clientX - r.left, e.clientY - r.top);
            } else if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
                e.preventDefault();
                view.x -= e.deltaX;
                view.touched = true;
                apply();
            }
        }, { passive: false });

        // Drag to pan (a click that moves no further than a few pixels still follows the link);
        // two pointers pinch.
        const down = new Map();
        let start = null, pinch = null, moved = false;
        const hideTip = () => { const t = document.querySelector(".ik-tooltip"); if (t) t.hidden = true; };
        const anchor = () => {
            const p = down.values().next().value;
            start = p && { x: p.x, y: p.y, vx: view.x, vy: view.y };
        };
        viewport.addEventListener("pointerdown", (e) => {
            if (e.pointerType === "mouse" && e.button !== 0) return;
            down.set(e.pointerId, { x: e.clientX, y: e.clientY });
            if (down.size === 1) moved = false;
            pinch = null;
            anchor();
        });
        viewport.addEventListener("pointermove", (e) => {
            if (!down.has(e.pointerId)) return;
            down.set(e.pointerId, { x: e.clientX, y: e.clientY });
            if (down.size > 1) {
                const [a, b] = Array.from(down.values());
                const r = viewport.getBoundingClientRect();
                const dist = Math.hypot(a.x - b.x, a.y - b.y);
                const mx = (a.x + b.x) / 2 - r.left, my = (a.y + b.y) / 2 - r.top;
                if (pinch) {
                    view.x += mx - pinch.mx;
                    view.y += my - pinch.my;
                    zoomAt(dist / pinch.dist, mx, my);
                }
                pinch = { dist, mx, my };
                moved = true;
                return;
            }
            const dx = e.clientX - start.x, dy = e.clientY - start.y;
            if (!moved && Math.hypot(dx, dy) < 4) return;
            if (!moved) {
                moved = true;
                viewport.setPointerCapture(e.pointerId);
                root.classList.add("is-panning");
                trace(null);
                hideTip();
            }
            view.x = start.vx + dx;
            view.y = start.vy + dy;
            view.touched = true;
            apply();
        });
        const up = (e) => {
            down.delete(e.pointerId);
            pinch = null;
            anchor();
            if (!down.size) root.classList.remove("is-panning");
        };
        viewport.addEventListener("pointerup", up);
        viewport.addEventListener("pointercancel", up);
        viewport.addEventListener("click", (e) => {
            if (moved) { e.preventDefault(); e.stopPropagation(); moved = false; }
        }, true);
        viewport.addEventListener("dragstart", (e) => e.preventDefault());
        // Focusing a node out of view scrolls the viewport; turn that into a pan.
        viewport.addEventListener("scroll", () => {
            view.x -= viewport.scrollLeft;
            view.y -= viewport.scrollTop;
            viewport.scrollLeft = viewport.scrollTop = 0;
            apply();
        });

        let traced = null;
        function trace(id) {
            if (id === traced) return;
            traced = id;
            root.classList.toggle("is-tracing", id != null);
            if (id == null) return;
            const walk = (map) => {
                const seen = new Set([id]), queue = [id];
                while (queue.length) for (const n of map.get(queue.shift()) || []) if (!seen.has(n)) { seen.add(n); queue.push(n); }
                return seen;
            };
            const upstream = walk(deps), downstream = walk(downs);
            els.forEach((el) => el.classList.toggle("is-lit", upstream.has(el.dataset.node) || downstream.has(el.dataset.node)));
            edgeEls.forEach((e) => e.el.classList.toggle("is-lit",
                (upstream.has(e.from) && upstream.has(e.to)) || (downstream.has(e.from) && downstream.has(e.to))));
        }
        const node = (e) => { const el = e.target.closest("[data-node]"); return el && el.dataset.node; };
        world.addEventListener("mouseover", (e) => { if (!down.size) trace(node(e)); });
        world.addEventListener("focusin", (e) => trace(node(e)));
        world.addEventListener("focusout", () => trace(null));
        viewport.addEventListener("mouseleave", () => trace(null));

        let width = 0;
        new ResizeObserver(() => {
            if (viewport.clientWidth === width) return;
            width = viewport.clientWidth;
            if (!view.built) build();
            else if (!view.touched) fit();
        }).observe(viewport);
        build();
        if (document.fonts) document.fonts.ready.then(() => { if (!view.touched) build(); });
    }

    // The workflow builder (builder.js) lays definitions out with the same layout.
    window.ikGraph = { layout, curve };

    const scan = () => document.querySelectorAll("[data-ik-graph]").forEach(init);
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", scan);
    else scan();
    if (!window.__ishikariGraphs) {
        window.__ishikariGraphs = true;
        document.addEventListener("htmx:afterSettle", scan);
    }
})();

// In a code textarea ([data-ik-code]), Tab indents by two spaces; after Escape, Tab moves
// focus on as usual.
(function () {
    if (window.__ishikariCode) return;
    window.__ishikariCode = true;
    let escaped = false;
    document.addEventListener("keydown", function (e) {
        const area = e.target.closest && e.target.closest("textarea[data-ik-code]");
        if (!area) return;
        if (e.key === "Escape") { escaped = true; return; }
        if (e.key !== "Tab" || e.shiftKey || e.altKey || e.ctrlKey || e.metaKey || escaped) {
            escaped = false;
            return;
        }
        e.preventDefault();
        // execCommand keeps the edit on the textarea's undo stack.
        if (!document.execCommand("insertText", false, "  ")) area.setRangeText("  ", area.selectionStart, area.selectionEnd, "end");
    });
})();
