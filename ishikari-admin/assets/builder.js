// The workflow builder ([data-ik-builder], on the definition editor). A definition's nodes sit
// on a canvas: HTML cards in a pannable, zoomable world, joined by SVG edges, with a <canvas>
// minimap. Node types come from a palette (click or drag), and a panel edits the selection: a
// node, an edge, or with nothing selected the workflow itself. Saves through the JSON API.
//
// Tidying lays the nodes out with admin.js's graph layout (window.ikGraph), when it's there.
(function () {
    "use strict";
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
    else init();

    function init() {
        const root = document.querySelector("[data-ik-builder]");
        if (!root || root.__ikBuilder) return;
        root.__ikBuilder = true;
        const NS = "http://www.w3.org/2000/svg";
        const SNAP = 10, MIN_K = 0.2, MAX_K = 2, PAD = 48;
        const ID_RE = /^[A-Za-z_][A-Za-z0-9_-]*$/;
        const FIELD_TYPES = ["string", "integer", "number", "boolean", "object", "array"];
        const TONES = {
            core: "primary", transform: "violet", http: "info", database: "warning",
            notification: "success", control: "neutral",
        };
        const CATEGORIES = ["core", "transform", "http", "database", "notification", "control"];
        const ICONS = {
            out: '<path d="M5 12h14"/>',
            in: '<path d="M5 12h14"/><path d="M12 5v14"/>',
            fit: '<path d="M8 3H5a2 2 0 0 0-2 2v3"/><path d="M21 8V5a2 2 0 0 0-2-2h-3"/><path d="M3 16v3a2 2 0 0 0 2 2h3"/><path d="M16 21h3a2 2 0 0 0 2-2v-3"/>',
            tidy: '<rect x="3" y="4" width="6" height="5" rx="1"/><rect x="15" y="4" width="6" height="5" rx="1"/><rect x="15" y="15" width="6" height="5" rx="1"/><path d="M9 6.5h6M12 6.5v11h3"/>',
        };
        const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
        const snap = (v) => Math.round(v / SNAP) * SNAP;
        const $ = (sel, el) => (el || root).querySelector(sel);
        const esc = (s) => String(s).replace(/[&<>"']/g, (c) =>
            ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
        const isEmpty = (v) => v === undefined || v === null || v === "";
        const hasExpr = (v) => typeof v === "string" && v.includes("{{");

        const basePath = root.dataset.basePath || "";
        const api = basePath + "/api";
        let definitionId = root.dataset.definitionId ? Number(root.dataset.definitionId) : null;

        const els = {
            name: $("[data-name]"), version: $("[data-version]"), status: $("[data-status]"),
            error: $("[data-error]"), palette: $("[data-palette]"), panel: $("[data-panel]"),
            canvas: $("[data-canvas]"), save: $('[data-action="save"]'),
            undo: $('[data-action="undo"]'), redo: $('[data-action="redo"]'),
        };

        // ---- State ---------------------------------------------------------------------------
        // nodes: id → { id, type, deps: [ids], inputs: {}, when, timeout, retries, x, y }
        let meta = { name: "New Workflow", description: "", version: 1, inputs: {}, outputs: {}, metadata: {} };
        let nodes = new Map();
        let selected = null; // { node: id } | { edge: [from, to] }
        let types = [];
        const typeOf = (name) => types.find((t) => t.name === name);

        function snapshot() {
            return JSON.stringify({ meta, nodes: Array.from(nodes.values()) });
        }
        function restore(s) {
            const o = JSON.parse(s);
            meta = o.meta;
            nodes = new Map(o.nodes.map((n) => [n.id, n]));
            if (selected && selected.node && !nodes.has(selected.node)) selected = null;
            if (selected && selected.edge && !hasEdge(selected.edge[0], selected.edge[1])) selected = null;
        }

        // Undo: a snapshot before each change. Edits to one field in a row are one step.
        const past = [], future = [];
        let lastKey = null, lastAt = 0;
        function record(key) {
            const now = Date.now();
            if (key && key === lastKey && now - lastAt < 1200) { lastAt = now; return; }
            past.push(snapshot());
            if (past.length > 200) past.shift();
            future.length = 0;
            lastKey = key || null;
            lastAt = now;
        }
        function undo() {
            if (!past.length) return;
            future.push(snapshot());
            restore(past.pop());
            lastKey = null;
            renderAll();
        }
        function redo() {
            if (!future.length) return;
            past.push(snapshot());
            restore(future.pop());
            lastKey = null;
            renderAll();
        }

        let saved = null; // the definition as last loaded or saved
        const dirty = () => saved !== JSON.stringify(toDefinition());

        // ---- Definition ⇄ state --------------------------------------------------------------
        function fromDefinition(def) {
            meta = {
                name: def.name || "New Workflow",
                description: def.description || "",
                version: def.version || 1,
                inputs: def.inputs || {},
                outputs: def.outputs || {},
                metadata: def.metadata || {},
            };
            nodes = new Map();
            Object.keys(def.nodes || {}).sort().forEach((id) => {
                const n = def.nodes[id];
                nodes.set(id, {
                    id, type: n.type, deps: (n.depends_on || []).slice(), inputs: n.inputs || {},
                    when: n.when || "", timeout: n.timeout_seconds ?? null, retries: n.max_retries ?? null,
                    x: 0, y: 0,
                });
            });
        }
        function toDefinition() {
            const out = {};
            nodes.forEach((n) => {
                const d = { type: n.type, depends_on: n.deps.filter((u) => nodes.has(u)), inputs: n.inputs };
                if (n.when) d.when = n.when;
                if (n.timeout != null) d.timeout_seconds = n.timeout;
                if (n.retries != null) d.max_retries = n.retries;
                out[n.id] = d;
            });
            const def = { name: meta.name.trim(), version: meta.version, inputs: meta.inputs, nodes: out, outputs: meta.outputs, metadata: meta.metadata };
            if (meta.description.trim()) def.description = meta.description.trim();
            return def;
        }

        // ---- Graph helpers --------------------------------------------------------------------
        const hasEdge = (a, b) => nodes.has(b) && nodes.get(b).deps.includes(a);
        function edges() {
            const list = [];
            nodes.forEach((n) => n.deps.forEach((u) => { if (nodes.has(u)) list.push([u, n.id]); }));
            return list;
        }
        // Everything `id` runs after, nearest first.
        function ancestors(id) {
            const seen = new Set(), queue = [id], order = [];
            while (queue.length) {
                const n = nodes.get(queue.shift());
                (n ? n.deps : []).forEach((u) => {
                    if (!seen.has(u) && nodes.has(u)) { seen.add(u); order.push(u); queue.push(u); }
                });
            }
            return order;
        }
        const wouldCycle = (from, to) => from === to || ancestors(from).includes(to);
        function missing(n) {
            const t = typeOf(n.type);
            if (!t) return [];
            return Object.keys(t.input_schema || {}).filter((k) => t.input_schema[k].required && isEmpty(n.inputs[k]));
        }
        function uniqueId(base) {
            base = String(base).replace(/[^A-Za-z0-9_-]+/g, "_").replace(/^[^A-Za-z_]+/, "") || "node";
            let id = base, i = 2;
            while (nodes.has(id)) id = base + "_" + i++;
            return id;
        }
        // References to node `from` in an expression, `{{nodes.from.…}}`, now name `to`.
        function rewrite(v, from, to) {
            if (typeof v === "string") {
                const re = new RegExp("(\\{\\{\\s*nodes\\.)" + from.replace(/[-]/g, "\\-") + "(?=[.\\s}])", "g");
                return v.replace(re, "$1" + to);
            }
            if (Array.isArray(v)) return v.map((x) => rewrite(x, from, to));
            if (v && typeof v === "object") {
                const o = {};
                for (const k in v) o[k] = rewrite(v[k], from, to);
                return o;
            }
            return v;
        }

        // ---- Mutations (each records an undo step and re-renders) ------------------------------
        function addNode(typeName, at, after) {
            record();
            const id = uniqueId(typeName);
            const n = { id, type: typeName, deps: [], inputs: {}, when: "", timeout: null, retries: null, x: snap(at.x), y: snap(at.y) };
            const t = typeOf(typeName);
            if (t) for (const k in t.input_schema || {}) {
                const d = t.input_schema[k].default;
                if (d !== undefined && d !== null) n.inputs[k] = d;
            }
            if (after && nodes.has(after)) n.deps.push(after);
            nodes.set(id, n);
            selected = { node: id };
            renderAll();
            return n;
        }
        function removeNode(id) {
            if (!nodes.has(id)) return;
            record();
            nodes.delete(id);
            nodes.forEach((n) => (n.deps = n.deps.filter((u) => u !== id)));
            selected = null;
            renderAll();
        }
        function connect(from, to) {
            if (!nodes.has(from) || !nodes.has(to) || hasEdge(from, to)) return false;
            if (wouldCycle(from, to)) {
                flash(`“${to}” already runs before “${from}”: connecting them would make a cycle.`);
                return false;
            }
            record();
            nodes.get(to).deps.push(from);
            selected = { edge: [from, to] };
            renderAll();
            return true;
        }
        function disconnect(from, to) {
            record();
            const n = nodes.get(to);
            if (n) n.deps = n.deps.filter((u) => u !== from);
            selected = null;
            renderAll();
        }
        function rename(from, to) {
            if (from === to) return true;
            if (!ID_RE.test(to)) return "Letters, digits, _ and -, starting with a letter or _.";
            if (nodes.has(to)) return `There is already a node “${to}”.`;
            record();
            const next = new Map();
            nodes.forEach((n, id) => {
                n.deps = n.deps.map((u) => (u === from ? to : u));
                n.inputs = rewrite(n.inputs, from, to);
                n.when = rewrite(n.when, from, to);
                if (id === from) n.id = to;
                next.set(n.id, n);
            });
            nodes = next;
            meta.outputs = rewrite(meta.outputs, from, to);
            selected = { node: to };
            renderAll();
            return true;
        }
        function deleteSelection() {
            if (!selected) return;
            if (selected.node) removeNode(selected.node);
            else disconnect(selected.edge[0], selected.edge[1]);
        }
        function select(sel) {
            selected = sel;
            renderSelection();
            renderPanel();
        }

        // ---- Canvas -----------------------------------------------------------------------------
        const canvas = els.canvas;
        canvas.innerHTML =
            '<div class="ik-graph-viewport ik-builder-viewport" tabindex="-1">' +
            '<div class="ik-builder-world"><svg class="ik-builder-edges" aria-hidden="true"></svg></div></div>' +
            '<div class="ik-builder-empty" hidden><p class="ik-empty-title">Start with a node</p>' +
            '<p class="ik-hint">Drag a node type here, or click one to add it.</p></div>' +
            '<div class="ik-graph-toolbar">' + tool("out", "Zoom out") +
            '<output title="Ctrl/⌘ + scroll or pinch to zoom">100%</output>' + tool("in", "Zoom in") +
            tool("fit", "Fit to view") + tool("tidy", "Tidy up the layout") + "</div>" +
            '<canvas class="ik-builder-minimap" aria-hidden="true"></canvas>';
        function tool(name, label) {
            return '<button type="button" data-tool="' + name + '" aria-label="' + label + '" title="' + label + '">' +
                '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
                ICONS[name] + "</svg></button>";
        }
        const viewport = $(".ik-builder-viewport", canvas);
        const world = $(".ik-builder-world", canvas);
        const edgeLayer = $(".ik-builder-edges", canvas);
        const empty = $(".ik-builder-empty", canvas);
        const readout = $(".ik-graph-toolbar output", canvas);
        const minimap = $(".ik-builder-minimap", canvas);
        const probe = document.createElement("span");
        probe.hidden = true;
        canvas.append(probe);
        const cards = new Map(); // id → element
        const view = { x: 0, y: 0, k: 1 };

        function apply() {
            world.style.transform = "translate(" + view.x + "px, " + view.y + "px) scale(" + view.k + ")";
            canvas.style.backgroundPosition = view.x + "px " + view.y + "px";
            canvas.style.backgroundSize = 20 * view.k + "px " + 20 * view.k + "px";
            readout.textContent = Math.round(view.k * 100) + "%";
            drawMinimap();
        }
        const toWorld = (cx, cy) => {
            const r = viewport.getBoundingClientRect();
            return { x: (cx - r.left - view.x) / view.k, y: (cy - r.top - view.y) / view.k };
        };
        function zoomAt(factor, cx, cy) {
            const k = clamp(view.k * factor, MIN_K, MAX_K);
            view.x = cx - (cx - view.x) * (k / view.k);
            view.y = cy - (cy - view.y) * (k / view.k);
            view.k = k;
            apply();
        }
        function bounds() {
            let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
            nodes.forEach((n) => {
                const s = size(n.id);
                x0 = Math.min(x0, n.x); y0 = Math.min(y0, n.y);
                x1 = Math.max(x1, n.x + s.w); y1 = Math.max(y1, n.y + s.h);
            });
            return x0 === Infinity ? null : { x0, y0, x1, y1 };
        }
        let fitWhenShown = false;
        function fit() {
            const b = bounds(), vw = viewport.clientWidth, vh = viewport.clientHeight;
            // Hidden (a background tab, a collapsed container): fit once it has a size.
            fitWhenShown = !vw || !vh;
            if (fitWhenShown) return;
            if (!b) { view.x = vw / 2; view.y = vh / 2; view.k = 1; return apply(); }
            const k = clamp(Math.min((vw - 2 * PAD) / (b.x1 - b.x0), (vh - 2 * PAD) / (b.y1 - b.y0), 1), MIN_K, 1);
            view.k = k;
            view.x = (vw - (b.x1 - b.x0) * k) / 2 - b.x0 * k;
            view.y = (vh - (b.y1 - b.y0) * k) / 2 - b.y0 * k;
            apply();
        }
        // Make sure node `id` is in view, panning as little as needed.
        function reveal(id) {
            const n = nodes.get(id), s = size(id);
            if (!n) return;
            const vw = viewport.clientWidth, vh = viewport.clientHeight;
            const left = n.x * view.k + view.x, top = n.y * view.k + view.y;
            const right = left + s.w * view.k, bottom = top + s.h * view.k;
            if (left < 16) view.x += 16 - left; else if (right > vw - 16) view.x -= right - (vw - 16);
            if (top < 56) view.y += 56 - top; else if (bottom > vh - 16) view.y -= bottom - (vh - 16);
            apply();
        }
        function size(id) {
            const el = cards.get(id);
            return el ? { w: el.offsetWidth, h: el.offsetHeight } : { w: 160, h: 56 };
        }

        // Lay every node out left to right with the run graph's layout.
        function tidy() {
            if (!nodes.size) return;
            const g = new Map();
            nodes.forEach((n) => { const s = size(n.id); g.set(n.id, { deps: n.deps.filter((u) => nodes.has(u)), w: s.w, h: s.h }); });
            if (window.ikGraph) {
                const out = window.ikGraph.layout(g);
                out.V.forEach((v) => { if (v.real) { const n = nodes.get(v.id); n.x = snap(v.x); n.y = snap(v.y - v.h / 2); } });
            } else {
                let i = 0;
                nodes.forEach((n) => { n.x = (i % 4) * 240; n.y = Math.floor(i / 4) * 120; i++; });
            }
        }

        function renderNodes() {
            cards.forEach((el, id) => { if (!nodes.has(id)) { el.remove(); cards.delete(id); } });
            nodes.forEach((n, id) => {
                let el = cards.get(id);
                if (!el) {
                    el = document.createElement("div");
                    el.className = "ik-graph-node ik-builder-node";
                    el.tabIndex = 0;
                    el.setAttribute("role", "button");
                    world.append(el);
                    cards.set(id, el);
                }
                const t = typeOf(n.type);
                const need = missing(n);
                el.dataset.node = id;
                el.className = "ik-graph-node ik-builder-node ik-tone-" + (t ? TONES[t.category] || "neutral" : "warning") +
                    (need.length ? " is-invalid" : "");
                el.setAttribute("aria-label", id + ", " + n.type);
                let metaLine = "";
                if (!t) metaLine = '<span class="ik-graph-node-meta is-warning">Unknown type</span>';
                else if (need.length) metaLine = '<span class="ik-graph-node-meta is-warning">Needs ' + esc(need.join(", ")) + "</span>";
                else if (n.when) metaLine = '<span class="ik-graph-node-meta" title="' + esc(n.when) + '">when …</span>';
                el.innerHTML =
                    '<span class="ik-builder-port is-in" data-port="in" title="Drag to an upstream node"></span>' +
                    '<span class="ik-graph-node-title">' + esc(id) + "</span>" +
                    '<span class="ik-graph-node-sub">' + esc(n.type) + "</span>" + metaLine +
                    '<span class="ik-builder-port is-out" data-port="out" title="Drag to a node that runs after this one"></span>';
                place(n);
            });
            empty.hidden = nodes.size > 0;
        }
        function place(n) {
            const el = cards.get(n.id);
            if (el) { el.style.left = n.x + "px"; el.style.top = n.y + "px"; }
        }

        function svg(tag, attrs) {
            const el = document.createElementNS(NS, tag);
            for (const k in attrs) el.setAttribute(k, attrs[k]);
            return el;
        }
        // From one point to another, leaving each horizontally; a backward edge loops wider.
        function path(a, b) {
            const c = Math.max(Math.abs(b.x - a.x) / 2, b.x < a.x ? 80 + Math.abs(b.y - a.y) / 3 : 48);
            return "M" + a.x + " " + a.y + " C" + (a.x + c) + " " + a.y + " " + (b.x - c) + " " + b.y + " " + b.x + " " + b.y;
        }
        const outPort = (id) => { const n = nodes.get(id), s = size(id); return { x: n.x + s.w, y: n.y + s.h / 2 }; };
        const inPort = (id) => { const n = nodes.get(id), s = size(id); return { x: n.x, y: n.y + s.h / 2 }; };
        let draft = null; // { from: point, to: point } while connecting

        function renderEdges() {
            // The layer covers the nodes (and a connection being drawn), in world coordinates.
            const b = bounds() || { x0: 0, y0: 0, x1: 1, y1: 1 };
            if (draft) {
                b.x0 = Math.min(b.x0, draft.a.x, draft.b.x); b.y0 = Math.min(b.y0, draft.a.y, draft.b.y);
                b.x1 = Math.max(b.x1, draft.a.x, draft.b.x); b.y1 = Math.max(b.y1, draft.a.y, draft.b.y);
            }
            const x = b.x0 - 200, y = b.y0 - 200, w = b.x1 - b.x0 + 400, h = b.y1 - b.y0 + 400;
            Object.assign(edgeLayer.style, { left: x + "px", top: y + "px", width: w + "px", height: h + "px" });
            edgeLayer.setAttribute("viewBox", x + " " + y + " " + w + " " + h);
            edgeLayer.replaceChildren();
            edges().forEach(([from, to]) => {
                const a = outPort(from), z = inPort(to);
                const tip = { x: z.x - 5, y: z.y };
                const d = path(a, tip);
                const g = svg("g", { class: "ik-builder-edge", "data-from": from, "data-to": to });
                g.append(
                    svg("path", { class: "ik-builder-edge-hit", d }),
                    svg("path", { class: "ik-builder-edge-line", d }),
                    svg("path", { class: "ik-builder-edge-arrow", d: "M" + (z.x - 7) + " " + (z.y - 4) + " L" + z.x + " " + z.y + " L" + (z.x - 7) + " " + (z.y + 4) + " Z" }));
                edgeLayer.append(g);
            });
            if (draft) edgeLayer.append(svg("path", { class: "ik-builder-edge-draft", d: draft.out ? path(draft.a, draft.b) : path(draft.b, draft.a) }));
            renderSelection();
        }
        function renderSelection() {
            cards.forEach((el, id) => el.classList.toggle("is-selected", !!(selected && selected.node === id)));
            edgeLayer.querySelectorAll(".ik-builder-edge").forEach((g) => g.classList.toggle("is-selected",
                !!(selected && selected.edge && selected.edge[0] === g.dataset.from && selected.edge[1] === g.dataset.to)));
        }

        // The minimap: every node, and the part of the canvas in view; click or drag to move there.
        let mapFrame = 0, mapScale = null;
        function drawMinimap() {
            if (mapFrame) return;
            mapFrame = requestAnimationFrame(() => {
                mapFrame = 0;
                const b = bounds();
                minimap.hidden = !b;
                if (!b) return;
                const dpr = window.devicePixelRatio || 1, cw = minimap.clientWidth, ch = minimap.clientHeight;
                if (minimap.width !== cw * dpr) { minimap.width = cw * dpr; minimap.height = ch * dpr; }
                const ctx = minimap.getContext("2d");
                ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
                ctx.clearRect(0, 0, cw, ch);
                const vw = viewport.clientWidth / view.k, vh = viewport.clientHeight / view.k;
                const vx = -view.x / view.k, vy = -view.y / view.k;
                const x0 = Math.min(b.x0, vx), y0 = Math.min(b.y0, vy);
                const x1 = Math.max(b.x1, vx + vw), y1 = Math.max(b.y1, vy + vh);
                const s = Math.min((cw - 12) / (x1 - x0), (ch - 12) / (y1 - y0));
                const ox = (cw - (x1 - x0) * s) / 2 - x0 * s, oy = (ch - (y1 - y0) * s) / 2 - y0 * s;
                mapScale = { s, ox, oy };
                // The tokens are light-dark() and the like, which a canvas can't read: resolve
                // them through an element's color.
                const color = (value) => { probe.style.color = value; return getComputedStyle(probe).color; };
                const selectedColor = color("var(--map-selected)");
                nodes.forEach((n) => {
                    const sz = size(n.id), el = cards.get(n.id);
                    ctx.fillStyle = selected && selected.node === n.id ? selectedColor
                        : color(el ? getComputedStyle(el).getPropertyValue("--ik-tone") : "currentColor");
                    ctx.globalAlpha = 0.75;
                    ctx.fillRect(ox + n.x * s, oy + n.y * s, Math.max(2, sz.w * s), Math.max(2, sz.h * s));
                });
                ctx.globalAlpha = 1;
                ctx.strokeStyle = getComputedStyle(minimap).color;
                ctx.lineWidth = 1;
                ctx.strokeRect(ox + vx * s + 0.5, oy + vy * s + 0.5, vw * s - 1, vh * s - 1);
            });
        }
        function mapTo(e) {
            if (!mapScale) return;
            const r = minimap.getBoundingClientRect();
            const wx = (e.clientX - r.left - mapScale.ox) / mapScale.s, wy = (e.clientY - r.top - mapScale.oy) / mapScale.s;
            view.x = viewport.clientWidth / 2 - wx * view.k;
            view.y = viewport.clientHeight / 2 - wy * view.k;
            apply();
        }
        minimap.addEventListener("pointerdown", (e) => {
            e.stopPropagation();
            minimap.setPointerCapture(e.pointerId);
            mapTo(e);
            const move = (ev) => mapTo(ev);
            minimap.addEventListener("pointermove", move);
            minimap.addEventListener("pointerup", () => minimap.removeEventListener("pointermove", move), { once: true });
        });

        $(".ik-graph-toolbar", canvas).addEventListener("click", (e) => {
            const b = e.target.closest("button");
            if (!b) return;
            const t = b.dataset.tool;
            if (t === "fit") return fit();
            if (t === "tidy") { record(); tidy(); renderAll(); return fit(); }
            zoomAt(t === "in" ? 1.25 : 0.8, viewport.clientWidth / 2, viewport.clientHeight / 2);
        });

        viewport.addEventListener("wheel", (e) => {
            e.preventDefault();
            const r = viewport.getBoundingClientRect();
            if (e.ctrlKey || e.metaKey) {
                const delta = clamp(e.deltaMode ? e.deltaY * 33 : e.deltaY, -120, 120);
                zoomAt(Math.exp(-delta * 0.0025), e.clientX - r.left, e.clientY - r.top);
            } else {
                const f = e.deltaMode ? 33 : 1;
                view.x -= (e.shiftKey && !e.deltaX ? e.deltaY : e.deltaX) * f;
                view.y -= (e.shiftKey && !e.deltaX ? 0 : e.deltaY) * f;
                apply();
            }
        }, { passive: false });
        // Focusing a node out of view scrolls the viewport; turn that into a pan.
        viewport.addEventListener("scroll", () => {
            view.x -= viewport.scrollLeft;
            view.y -= viewport.scrollTop;
            viewport.scrollLeft = viewport.scrollTop = 0;
            apply();
        });

        // Pointers: drag a node to move it, a port to connect, the background to pan; two
        // pointers pinch. A press that doesn't move selects (or, on the background, deselects).
        const pointers = new Map();
        let gesture = null;
        viewport.addEventListener("pointerdown", (e) => {
            if (e.pointerType === "mouse" && e.button !== 0) return;
            pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
            if (pointers.size > 1) {
                if (gesture && gesture.kind === "connect") { draft = null; renderEdges(); }
                const [a, b] = Array.from(pointers.values());
                gesture = { kind: "pinch", dist: Math.hypot(a.x - b.x, a.y - b.y), mx: (a.x + b.x) / 2, my: (a.y + b.y) / 2 };
                return;
            }
            const port = e.target.closest("[data-port]");
            const card = e.target.closest("[data-node]");
            const edge = e.target.closest(".ik-builder-edge");
            const start = { x: e.clientX, y: e.clientY };
            if (port && card) {
                e.preventDefault();
                const id = card.dataset.node, out = port.dataset.port === "out";
                const p = out ? outPort(id) : inPort(id);
                gesture = { kind: "connect", id, out, start };
                draft = { out, a: p, b: toWorld(e.clientX, e.clientY) };
                viewport.setPointerCapture(e.pointerId);
                root.classList.add("is-connecting");
                renderEdges();
            } else if (card) {
                const n = nodes.get(card.dataset.node);
                gesture = { kind: "node", id: n.id, start, from: { x: n.x, y: n.y }, moved: false };
            } else if (edge) {
                gesture = { kind: "edge", edge: [edge.dataset.from, edge.dataset.to], start, moved: false };
            } else {
                gesture = { kind: "pan", start, from: { x: view.x, y: view.y }, moved: false };
            }
        });
        viewport.addEventListener("pointermove", (e) => {
            if (!pointers.has(e.pointerId) || !gesture) return;
            pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
            if (gesture.kind === "pinch") {
                if (pointers.size < 2) return;
                const [a, b] = Array.from(pointers.values());
                const r = viewport.getBoundingClientRect();
                const dist = Math.hypot(a.x - b.x, a.y - b.y), mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
                view.x += mx - gesture.mx;
                view.y += my - gesture.my;
                zoomAt(dist / gesture.dist, mx - r.left, my - r.top);
                Object.assign(gesture, { dist, mx, my });
                return;
            }
            const dx = e.clientX - gesture.start.x, dy = e.clientY - gesture.start.y;
            if (gesture.kind === "connect") {
                draft.b = toWorld(e.clientX, e.clientY);
                const over = document.elementFromPoint(e.clientX, e.clientY);
                const target = over && over.closest && over.closest("[data-node]");
                cards.forEach((el, id) => el.classList.toggle("is-target", !!target && target === el && id !== gesture.id));
                renderEdges();
                return;
            }
            if (!gesture.moved && Math.hypot(dx, dy) < 4) return;
            if (!gesture.moved) {
                gesture.moved = true;
                viewport.setPointerCapture(e.pointerId);
                if (gesture.kind === "node") { record(); cards.get(gesture.id).classList.add("is-dragging"); }
                else root.classList.add("is-panning");
            }
            if (gesture.kind === "node") {
                const n = nodes.get(gesture.id);
                n.x = snap(gesture.from.x + dx / view.k);
                n.y = snap(gesture.from.y + dy / view.k);
                place(n);
                renderEdges();
                drawMinimap();
            } else if (gesture.kind === "pan" || gesture.kind === "edge") {
                gesture.kind = "pan";
                gesture.from = gesture.from || { x: view.x - dx, y: view.y - dy };
                view.x = gesture.from.x + dx;
                view.y = gesture.from.y + dy;
                apply();
            }
        });
        function release(e) {
            if (!pointers.has(e.pointerId)) return;
            pointers.delete(e.pointerId);
            const g = gesture;
            if (pointers.size) return; // the rest of a pinch
            gesture = null;
            root.classList.remove("is-panning", "is-connecting");
            if (!g || e.type === "pointercancel") { if (draft) { draft = null; renderEdges(); } return; }
            if (g.kind === "connect") {
                cards.forEach((el) => el.classList.remove("is-target"));
                draft = null;
                const over = document.elementFromPoint(e.clientX, e.clientY);
                const target = over && over.closest && over.closest("[data-node]");
                if (target && target.dataset.node !== g.id) {
                    const other = target.dataset.node;
                    if (!(g.out ? connect(g.id, other) : connect(other, g.id))) renderEdges();
                } else renderEdges();
            } else if (g.kind === "node") {
                cards.get(g.id).classList.remove("is-dragging");
                if (g.moved) { renderPanelStatus(); drawMinimap(); }
                else { select({ node: g.id }); }
            } else if (g.kind === "edge" && !g.moved) {
                select({ edge: g.edge });
            } else if (g.kind === "pan" && !g.moved) {
                select(null);
            }
        }
        viewport.addEventListener("pointerup", release);
        viewport.addEventListener("pointercancel", release);
        viewport.addEventListener("dragstart", (e) => { if (!e.target.closest || !e.target.closest("[data-type]")) e.preventDefault(); });
        world.addEventListener("keydown", (e) => {
            const card = e.target.closest("[data-node]");
            if (card && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); select({ node: card.dataset.node }); }
        });
        world.addEventListener("focusin", (e) => {
            const card = e.target.closest("[data-node]");
            if (card) reveal(card.dataset.node);
        });

        // Dropping a node type from the palette.
        viewport.addEventListener("dragover", (e) => {
            if (!e.dataTransfer.types.includes("application/x-ishikari-node-type")) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = "copy";
        });
        viewport.addEventListener("drop", (e) => {
            const name = e.dataTransfer.getData("application/x-ishikari-node-type");
            if (!name) return;
            e.preventDefault();
            const p = toWorld(e.clientX, e.clientY);
            addNode(name, { x: p.x - 80, y: p.y - 24 });
        });

        // ---- Palette ----------------------------------------------------------------------------
        function renderPalette() {
            const groups = new Map();
            types.forEach((t) => (groups.get(t.category) || groups.set(t.category, []).get(t.category)).push(t));
            const order = Array.from(groups.keys()).sort((a, b) => {
                const i = CATEGORIES.indexOf(a), j = CATEGORIES.indexOf(b);
                return (i < 0 ? 99 : i) - (j < 0 ? 99 : j) || a.localeCompare(b);
            });
            els.palette.innerHTML =
                '<div class="ik-builder-palette-head"><input type="search" class="ik-input ik-input-sm" placeholder="Find a node type" aria-label="Find a node type" data-filter></div>' +
                order.map((c) =>
                    '<section class="ik-builder-group" data-group>' +
                    '<h3 class="ik-builder-heading">' + esc(c) + "</h3>" +
                    groups.get(c).map((t) =>
                        '<button type="button" class="ik-builder-type ik-tone-' + (TONES[c] || "neutral") + '" draggable="true" data-type="' + esc(t.name) + '"' +
                        ' data-search="' + esc((t.name + " " + (t.description || "") + " " + c).toLowerCase()) + '">' +
                        '<span class="ik-builder-type-name">' + esc(t.name) + "</span>" +
                        (t.description ? '<span class="ik-builder-type-desc">' + esc(t.description) + "</span>" : "") +
                        "</button>").join("") +
                    "</section>").join("") +
                '<p class="ik-hint ik-builder-palette-foot">Click to add (after the selected node), or drag onto the canvas.</p>';
        }
        els.palette.addEventListener("input", (e) => {
            if (!e.target.matches("[data-filter]")) return;
            const q = e.target.value.trim().toLowerCase();
            els.palette.querySelectorAll("[data-group]").forEach((g) => {
                let any = false;
                g.querySelectorAll("[data-type]").forEach((b) => { const hit = !q || b.dataset.search.includes(q); b.hidden = !hit; any = any || hit; });
                g.hidden = !any;
            });
        });
        els.palette.addEventListener("dragstart", (e) => {
            const b = e.target.closest("[data-type]");
            if (!b) return;
            e.dataTransfer.setData("application/x-ishikari-node-type", b.dataset.type);
            e.dataTransfer.effectAllowed = "copy";
        });
        els.palette.addEventListener("click", (e) => {
            const b = e.target.closest("[data-type]");
            if (!b) return;
            // After the selected node, to its right; else in the middle of the view, clear of others.
            const after = selected && selected.node && nodes.has(selected.node) ? selected.node : null;
            let at;
            if (after) {
                const a = nodes.get(after), s = size(after);
                at = { x: a.x + s.w + 88, y: a.y };
            } else {
                at = toWorld(viewport.getBoundingClientRect().left + viewport.clientWidth / 2, viewport.getBoundingClientRect().top + viewport.clientHeight / 2);
                at.x -= 80; at.y -= 24;
            }
            while (Array.from(nodes.values()).some((n) => Math.abs(n.x - at.x) < 140 && Math.abs(n.y - at.y) < 60)) at.y += 80;
            const n = addNode(b.dataset.type, at, after);
            reveal(n.id);
        });

        // ---- Panel ------------------------------------------------------------------------------
        const field = (label, control, hint) =>
            '<label class="ik-field"><span class="ik-label">' + label + "</span>" + control +
            (hint ? '<span class="ik-hint">' + hint + "</span>" : "") + "</label>";

        function renderPanel() {
            closeMenu();
            if (selected && selected.node && nodes.has(selected.node)) renderNodePanel(nodes.get(selected.node));
            else if (selected && selected.edge) renderEdgePanel(selected.edge);
            else renderWorkflowPanel();
        }

        function renderWorkflowPanel() {
            const inputs = Object.keys(meta.inputs), outputs = Object.keys(meta.outputs);
            els.panel.innerHTML =
                '<div class="ik-builder-section"><h2 class="ik-builder-title">Workflow</h2>' +
                field("Name", '<input class="ik-input ik-input-sm" data-meta="name" value="' + esc(meta.name) + '" required>') +
                field("Description", '<textarea class="ik-textarea" rows="2" data-meta="description">' + esc(meta.description) + "</textarea>") +
                field("Version", '<input class="ik-input ik-input-sm" type="number" min="1" step="1" data-meta="version" value="' + esc(meta.version) + '">') +
                "</div>" +
                '<div class="ik-builder-section"><div class="ik-builder-section-head"><h3 class="ik-builder-heading">Inputs</h3>' +
                '<button type="button" class="ik-btn ik-btn-ghost ik-btn-xs" data-add="input">Add</button></div>' +
                (inputs.length ? inputs.map((name) => inputRow(name, meta.inputs[name])).join("") :
                    '<p class="ik-hint">What a run is given, as <code class="ik-code-inline">{{inputs.name}}</code>.</p>') +
                "</div>" +
                '<div class="ik-builder-section"><div class="ik-builder-section-head"><h3 class="ik-builder-heading">Outputs</h3>' +
                '<button type="button" class="ik-btn ik-btn-ghost ik-btn-xs" data-add="output">Add</button></div>' +
                (outputs.length ? outputs.map((name) =>
                    '<div class="ik-builder-card" data-output="' + esc(name) + '">' +
                    '<div class="ik-builder-card-head"><input class="ik-input ik-input-sm ik-mono" aria-label="Output name" data-output-name value="' + esc(name) + '">' +
                    '<button type="button" class="ik-btn ik-btn-ghost ik-btn-xs ik-btn-icon" aria-label="Remove output ' + esc(name) + '" data-remove-output>' + X + "</button></div>" +
                    exprInput('data-output-value', meta.outputs[name], "{{nodes.id.output}}", "Value of " + name) + "</div>").join("") :
                    '<p class="ik-hint">What a run returns, each an expression.</p>') +
                "</div>" + syntaxHelp() + '<div class="ik-builder-section ik-hint" data-panel-status></div>';
            renderPanelStatus();
        }
        function inputRow(name, f) {
            return '<div class="ik-builder-card" data-input="' + esc(name) + '">' +
                '<div class="ik-builder-card-head"><input class="ik-input ik-input-sm ik-mono" aria-label="Input name" data-input-name value="' + esc(name) + '">' +
                '<button type="button" class="ik-btn ik-btn-ghost ik-btn-xs ik-btn-icon" aria-label="Remove input ' + esc(name) + '" data-remove-input>' + X + "</button></div>" +
                '<div class="ik-builder-pair"><select class="ik-select ik-input-sm" aria-label="Type" data-input-field="type">' +
                FIELD_TYPES.map((t) => "<option" + (t === f.type ? " selected" : "") + ">" + t + "</option>").join("") + "</select>" +
                '<label class="ik-builder-check"><input type="checkbox" data-input-field="required"' + (f.required ? " checked" : "") + "> Required</label></div>" +
                '<input class="ik-input ik-input-sm" placeholder="Default" aria-label="Default" data-input-field="default" value="' +
                esc(f.default === undefined || f.default === null ? "" : typeof f.default === "string" ? f.default : JSON.stringify(f.default)) + '">' +
                '<input class="ik-input ik-input-sm" placeholder="Description" aria-label="Description" data-input-field="description" value="' + esc(f.description || "") + '">' +
                "</div>";
        }
        const X = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M18 6 6 18M6 6l12 12"/></svg>';
        function syntaxHelp() {
            return '<div class="ik-builder-section"><h3 class="ik-builder-heading">Expressions</h3>' +
                '<p class="ik-hint"><code class="ik-code-inline">{{inputs.name}}</code> a workflow input</p>' +
                '<p class="ik-hint"><code class="ik-code-inline">{{nodes.id.output}}</code> an upstream node\'s output</p></div>';
        }
        function exprInput(attr, value, placeholder, label, multiline) {
            const v = value === undefined || value === null ? "" : typeof value === "string" ? value : JSON.stringify(value, null, multiline ? 2 : 0);
            const cls = hasExpr(v) ? " is-expr" : "";
            const control = multiline
                ? '<textarea class="ik-textarea ik-mono' + cls + '" rows="3" ' + attr + ' aria-label="' + esc(label) + '" placeholder="' + esc(placeholder) + '">' + esc(v) + "</textarea>"
                : '<input class="ik-input ik-input-sm' + cls + '" ' + attr + ' aria-label="' + esc(label) + '" placeholder="' + esc(placeholder) + '" value="' + esc(v) + '">';
            return '<div class="ik-builder-expr">' + control +
                '<button type="button" class="ik-builder-insert" data-insert title="Insert a reference" aria-label="Insert a reference into ' + esc(label) + '">{{ }}</button></div>';
        }

        function renderNodePanel(n) {
            const t = typeOf(n.type);
            const schema = (t && t.input_schema) || {};
            const keys = Object.keys(schema).sort((a, b) => (schema[b].required ? 1 : 0) - (schema[a].required ? 1 : 0) || a.localeCompare(b));
            Object.keys(n.inputs).forEach((k) => { if (!keys.includes(k)) keys.push(k); });
            const outs = t && t.output_schema ? Object.keys(t.output_schema).sort() : [];
            els.panel.innerHTML =
                '<div class="ik-builder-section"><div class="ik-builder-section-head">' +
                '<h2 class="ik-builder-title ik-mono">' + esc(n.id) + "</h2>" +
                '<button type="button" class="ik-btn ik-btn-ghost ik-btn-xs" data-delete-node>Delete</button></div>' +
                '<p class="ik-hint">' + esc(n.type) + (t && t.description ? " · " + esc(t.description) : "") + (t ? "" : " · not a known node type") + "</p>" +
                field("ID", '<input class="ik-input ik-input-sm ik-mono" data-node-id value="' + esc(n.id) + '" spellcheck="false" autocomplete="off">',
                    "Other nodes refer to its outputs by it.") +
                "</div>" +
                '<div class="ik-builder-section"><h3 class="ik-builder-heading">Inputs</h3>' +
                (keys.length ? keys.map((k) => nodeInput(n, k, schema[k])).join("") : '<p class="ik-hint">This node type takes no inputs.</p>') +
                "</div>" +
                '<div class="ik-builder-section"><h3 class="ik-builder-heading">Runs after</h3>' +
                (n.deps.length ? '<ul class="ik-builder-list">' + n.deps.map((u) =>
                    '<li><button type="button" class="ik-link ik-mono" data-goto="' + esc(u) + '">' + esc(u) + "</button>" +
                    '<button type="button" class="ik-btn ik-btn-ghost ik-btn-xs ik-btn-icon" aria-label="Stop running after ' + esc(u) + '" data-unlink="' + esc(u) + '">' + X + "</button></li>").join("") + "</ul>"
                    : '<p class="ik-hint">Nothing: it runs first. Drag from a node\'s right port to this one to run it after.</p>') +
                "</div>" +
                '<div class="ik-builder-section"><h3 class="ik-builder-heading">Execution</h3>' +
                field("When", exprInput("data-node-field=\"when\"", n.when, "{{nodes.check.ok}}", "When"), "Runs only if this is truthy (not false, empty or 0).") +
                '<div class="ik-builder-pair">' +
                field("Timeout (s)", '<input class="ik-input ik-input-sm" type="number" min="1" step="1" placeholder="None" data-node-field="timeout" value="' + esc(n.timeout ?? "") + '">') +
                field("Max retries", '<input class="ik-input ik-input-sm" type="number" min="0" step="1" placeholder="Default" data-node-field="retries" value="' + esc(n.retries ?? "") + '">') +
                "</div></div>" +
                (outs.length ? '<div class="ik-builder-section"><h3 class="ik-builder-heading">Outputs</h3><ul class="ik-builder-list">' +
                    outs.map((o) => '<li><code class="ik-code-inline">{{nodes.' + esc(n.id) + "." + esc(o) + '}}</code><span class="ik-hint">' + esc(t.output_schema[o].type) + "</span></li>").join("") +
                    "</ul></div>" : "") +
                '<div class="ik-builder-section ik-hint" data-panel-status></div>';
            renderPanelStatus();
        }
        function nodeInput(n, k, f) {
            f = f || { type: "string" };
            const v = n.inputs[k];
            const label = esc(k) + (f.required ? ' <span class="ik-builder-required" title="Required">*</span>' : "") +
                (f.type ? ' <span class="ik-builder-type-tag">' + esc(f.type) + "</span>" : "");
            const attr = 'data-node-input="' + esc(k) + '"';
            let control;
            if (f.type === "boolean" && !hasExpr(v)) {
                control = '<label class="ik-builder-check"><input type="checkbox" ' + attr + (v === true || v === "true" ? " checked" : "") + "> " + esc(f.description || k) + "</label>";
                return '<div class="ik-field"><span class="ik-label">' + label + "</span>" + control + "</div>";
            }
            control = exprInput(attr, v, f.description || k, k, f.type === "object" || f.type === "array");
            const missingHint = f.required && isEmpty(v) ? '<span class="ik-hint is-warning">Required</span>' : "";
            return '<div class="ik-field"><span class="ik-label">' + label + "</span>" + control +
                (f.description && f.type !== "boolean" ? '<span class="ik-hint">' + esc(f.description) + "</span>" : "") + missingHint + "</div>";
        }
        function renderEdgePanel([from, to]) {
            els.panel.innerHTML =
                '<div class="ik-builder-section"><h2 class="ik-builder-title">Dependency</h2>' +
                '<p class="ik-builder-edge-desc"><button type="button" class="ik-link ik-mono" data-goto="' + esc(from) + '">' + esc(from) + "</button>" +
                '<span aria-hidden="true">→</span><button type="button" class="ik-link ik-mono" data-goto="' + esc(to) + '">' + esc(to) + "</button></p>" +
                '<p class="ik-hint">' + esc(to) + " runs once " + esc(from) + " completes, and is cancelled if it fails.</p>" +
                '<button type="button" class="ik-btn ik-btn-danger ik-btn-sm" data-delete-edge>Remove</button></div>' +
                '<div class="ik-builder-section ik-hint" data-panel-status></div>';
            renderPanelStatus();
        }
        function renderPanelStatus() {
            const el = els.panel.querySelector("[data-panel-status]");
            if (!el) return;
            const need = Array.from(nodes.values()).filter((n) => missing(n).length).map((n) => n.id);
            el.textContent = nodes.size + (nodes.size === 1 ? " node, " : " nodes, ") + edges().length +
                (edges().length === 1 ? " dependency." : " dependencies.") +
                (need.length ? " Missing required inputs: " + need.join(", ") + "." : "");
        }

        // Panel edits: typing updates the state (and the canvas) without redrawing the panel.
        function parseValue(raw, type) {
            if (hasExpr(raw)) return raw;
            if (raw === "") return undefined;
            if (type === "integer" || type === "number") { const x = Number(raw); return Number.isFinite(x) && raw.trim() !== "" ? x : raw; }
            if (type === "object" || type === "array") { try { return JSON.parse(raw); } catch (_) { return raw; } }
            return raw;
        }
        const intOrNull = (raw) => { const x = parseInt(raw, 10); return Number.isFinite(x) && x >= 0 ? x : null; };
        function refreshCanvas() { renderNodes(); renderEdges(); drawMinimap(); renderPanelStatus(); updateDirty(); }

        els.panel.addEventListener("input", (e) => {
            const el = e.target;
            if (el.matches("input:not([type=checkbox]), textarea")) el.classList.toggle("is-expr", hasExpr(el.value));
            const n = selected && selected.node ? nodes.get(selected.node) : null;
            if (el.dataset.meta) {
                record("meta:" + el.dataset.meta);
                if (el.dataset.meta === "version") meta.version = Math.max(1, parseInt(el.value, 10) || 1);
                else meta[el.dataset.meta] = el.value;
                renderHeader();
                return updateDirty();
            }
            if (n && el.dataset.nodeInput !== undefined) {
                const k = el.dataset.nodeInput, t = typeOf(n.type);
                const f = (t && t.input_schema && t.input_schema[k]) || { type: "string" };
                record("input:" + n.id + ":" + k);
                const v = el.type === "checkbox" ? el.checked : parseValue(el.value, f.type);
                if (v === undefined) delete n.inputs[k]; else n.inputs[k] = v;
                return refreshCanvas();
            }
            if (n && el.dataset.nodeField) {
                const k = el.dataset.nodeField;
                record("field:" + n.id + ":" + k);
                if (k === "when") n.when = el.value;
                else n[k] = intOrNull(el.value);
                return refreshCanvas();
            }
            const row = el.closest("[data-input]");
            if (row && el.dataset.inputField) {
                const f = meta.inputs[row.dataset.input];
                record("winput:" + row.dataset.input + ":" + el.dataset.inputField);
                const k = el.dataset.inputField;
                if (k === "required") f.required = el.checked;
                else if (k === "type") f.type = el.value;
                else if (k === "default") { const v = parseValue(el.value, f.type); if (v === undefined) delete f.default; else f.default = v; }
                else if (el.value) f[k] = el.value; else delete f[k];
                return updateDirty();
            }
            const out = el.closest("[data-output]");
            if (out && el.dataset.outputValue !== undefined) {
                record("output:" + out.dataset.output);
                meta.outputs[out.dataset.output] = el.value;
                return updateDirty();
            }
        });
        // Names and IDs change on commit (blur or Enter), since every reference follows them.
        els.panel.addEventListener("change", (e) => {
            const el = e.target;
            if (el.matches("[data-node-id]")) {
                const ok = rename(selected.node, el.value.trim());
                if (ok !== true) { flash(ok); el.value = selected.node; }
                return;
            }
            if (el.matches("[data-input-name]")) return renameKey("inputs", el.closest("[data-input]").dataset.input, el.value.trim(), el);
            if (el.matches("[data-output-name]")) return renameKey("outputs", el.closest("[data-output]").dataset.output, el.value.trim(), el);
        });
        els.panel.addEventListener("keydown", (e) => {
            if (e.key === "Enter" && e.target.matches("input[data-node-id], input[data-input-name], input[data-output-name]")) e.target.blur();
        });
        function renameKey(which, from, to, el) {
            if (from === to) return;
            if (!ID_RE.test(to) || to in meta[which]) {
                flash(!ID_RE.test(to) ? "Names are letters, digits, _ and -, starting with a letter or _." : `There is already an ${which.slice(0, -1)} “${to}”.`);
                el.value = from;
                return;
            }
            record();
            const next = {};
            for (const k in meta[which]) next[k === from ? to : k] = meta[which][k];
            meta[which] = next;
            if (which === "inputs") {
                const re = new RegExp("(\\{\\{\\s*inputs\\.)" + from.replace(/[-]/g, "\\-") + "(?=[.\\s}])", "g");
                const fix = (v) => typeof v === "string" ? v.replace(re, "$1" + to) : Array.isArray(v) ? v.map(fix) :
                    v && typeof v === "object" ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, fix(x)])) : v;
                nodes.forEach((n) => { n.inputs = fix(n.inputs); n.when = fix(n.when); });
                meta.outputs = fix(meta.outputs);
            }
            renderAll();
        }
        els.panel.addEventListener("click", (e) => {
            const b = e.target.closest("button");
            if (!b) return;
            if (b.matches("[data-insert]")) return openMenu(b);
            if (b.matches("[data-delete-node]")) return removeNode(selected.node);
            if (b.matches("[data-delete-edge]")) return disconnect(selected.edge[0], selected.edge[1]);
            if (b.dataset.unlink) return disconnect(b.dataset.unlink, selected.node);
            if (b.dataset.goto) { select({ node: b.dataset.goto }); return reveal(b.dataset.goto); }
            if (b.dataset.add === "input") {
                record();
                let name = "input", i = 2;
                while (name in meta.inputs) name = "input_" + i++;
                meta.inputs[name] = { type: "string", required: false };
                renderPanel();
                updateDirty();
                const el = els.panel.querySelector('[data-input="' + name + '"] [data-input-name]');
                if (el) { el.focus(); el.select(); }
                return;
            }
            if (b.dataset.add === "output") {
                record();
                let name = "result", i = 2;
                while (name in meta.outputs) name = "result_" + i++;
                meta.outputs[name] = "";
                renderPanel();
                updateDirty();
                const el = els.panel.querySelector('[data-output="' + name + '"] [data-output-name]');
                if (el) { el.focus(); el.select(); }
                return;
            }
            if (b.matches("[data-remove-input]")) { record(); delete meta.inputs[b.closest("[data-input]").dataset.input]; renderPanel(); return updateDirty(); }
            if (b.matches("[data-remove-output]")) { record(); delete meta.outputs[b.closest("[data-output]").dataset.output]; renderPanel(); return updateDirty(); }
        });

        // The reference menu: workflow inputs and the outputs of the nodes this one runs after.
        let menu = null;
        function openMenu(button) {
            closeMenu();
            const target = button.parentElement.querySelector("input, textarea");
            const n = selected && selected.node ? nodes.get(selected.node) : null;
            const ups = n ? ancestors(n.id) : Array.from(nodes.keys());
            const groups = [];
            const ins = Object.keys(meta.inputs);
            if (ins.length) groups.push(["Workflow inputs", ins.map((k) => "{{inputs." + k + "}}")]);
            ups.forEach((u) => {
                const t = typeOf(nodes.get(u).type);
                const outs = t && t.output_schema && Object.keys(t.output_schema).length ? Object.keys(t.output_schema).sort() : ["result"];
                groups.push([u, outs.map((o) => "{{nodes." + u + "." + o + "}}")]);
            });
            menu = document.createElement("div");
            menu.className = "ik-builder-menu";
            menu.setAttribute("role", "menu");
            menu.innerHTML = groups.length ? groups.map(([title, refs]) =>
                '<div class="ik-builder-menu-group"><span class="ik-builder-heading">' + esc(title) + "</span>" +
                refs.map((r) => '<button type="button" role="menuitem" data-ref="' + esc(r) + '">' + esc(r) + "</button>").join("") + "</div>").join("")
                : '<p class="ik-hint">Nothing to refer to yet: add workflow inputs, or connect upstream nodes.</p>';
            root.append(menu);
            const r = button.getBoundingClientRect(), m = menu.getBoundingClientRect();
            menu.style.left = Math.max(8, Math.min(r.right - m.width, window.innerWidth - m.width - 8)) + "px";
            menu.style.top = (r.bottom + 4 + m.height > window.innerHeight ? r.top - m.height - 4 : r.bottom + 4) + "px";
            menu.addEventListener("click", (e) => {
                const item = e.target.closest("[data-ref]");
                if (!item) return;
                const start = target.selectionStart ?? target.value.length, end = target.selectionEnd ?? target.value.length;
                target.value = target.value.slice(0, start) + item.dataset.ref + target.value.slice(end);
                target.focus();
                target.selectionStart = target.selectionEnd = start + item.dataset.ref.length;
                target.dispatchEvent(new Event("input", { bubbles: true }));
                closeMenu();
            });
            const first = menu.querySelector("button");
            if (first) first.focus();
        }
        function closeMenu() { if (menu) { menu.remove(); menu = null; } }
        document.addEventListener("pointerdown", (e) => { if (menu && !menu.contains(e.target) && !e.target.closest("[data-insert]")) closeMenu(); });

        // ---- Header, saving -----------------------------------------------------------------------
        function renderHeader() {
            els.name.textContent = meta.name.trim() || "Untitled workflow";
            els.version.textContent = "v" + meta.version;
        }
        function updateDirty() {
            const d = dirty();
            root.classList.toggle("is-dirty", d);
            els.status.textContent = !definitionId ? "Not saved yet" : d ? "Unsaved changes" : "Saved";
            els.undo.disabled = !past.length;
            els.redo.disabled = !future.length;
        }
        let flashTimer = 0;
        function flash(message, tone) {
            els.error.hidden = false;
            els.error.className = "ik-alert " + (tone === "ok" ? "ik-tone-success" : "ik-alert-error") + " ik-builder-alert";
            els.error.textContent = message;
            clearTimeout(flashTimer);
            flashTimer = setTimeout(() => (els.error.hidden = true), tone === "ok" ? 2500 : 8000);
        }

        let saving = false;
        async function save() {
            if (saving) return;
            if (!meta.name.trim()) { select(null); return flash("Give the workflow a name before saving."); }
            if (!nodes.size) return flash("Add at least one node before saving.");
            saving = true;
            els.save.disabled = true;
            els.save.textContent = "Saving…";
            const def = toDefinition();
            try {
                const res = await fetch(definitionId ? api + "/definitions/" + definitionId : api + "/definitions", {
                    method: definitionId ? "PUT" : "POST",
                    headers: { "Content-Type": "application/json", Accept: "application/json" },
                    body: JSON.stringify(def),
                });
                if (!res.ok) throw new Error((await res.text()) || res.statusText);
                const detail = await res.json();
                saved = JSON.stringify(def);
                if (!definitionId) {
                    definitionId = detail.id;
                    root.dataset.definitionId = detail.id;
                    history.replaceState(null, "", basePath + "/definitions/" + detail.id + "/edit");
                    const view = root.querySelector("[data-view-link]");
                    if (view) { view.href = basePath + "/definitions/" + detail.id; view.hidden = false; }
                }
                els.error.hidden = true;
                flash("Saved " + (meta.name.trim()) + ".", "ok");
            } catch (err) {
                flash("Couldn't save: " + err.message);
            } finally {
                saving = false;
                els.save.disabled = false;
                els.save.textContent = "Save";
                updateDirty();
            }
        }
        root.addEventListener("click", (e) => {
            const b = e.target.closest("[data-action]");
            if (!b) return;
            if (b.dataset.action === "save") save();
            else if (b.dataset.action === "undo") undo();
            else if (b.dataset.action === "redo") redo();
        });
        window.addEventListener("beforeunload", (e) => { if (saved !== null && dirty()) { e.preventDefault(); e.returnValue = ""; } });

        // Keys: delete the selection, undo/redo, save, nudge a node, and Escape.
        document.addEventListener("keydown", (e) => {
            const mod = e.metaKey || e.ctrlKey;
            const typing = e.target.closest && e.target.closest("input, textarea, select, [contenteditable]");
            if (mod && e.key.toLowerCase() === "s") { e.preventDefault(); if (typing) typing.blur(); return save(); }
            if (e.key === "Escape") {
                if (menu) return closeMenu();
                if (gesture && gesture.kind === "connect") { gesture = null; draft = null; root.classList.remove("is-connecting"); return renderEdges(); }
                if (!typing && selected) return select(null);
            }
            if (typing) return;
            if (mod && e.key.toLowerCase() === "z") { e.preventDefault(); return e.shiftKey ? redo() : undo(); }
            if (mod && e.key.toLowerCase() === "y") { e.preventDefault(); return redo(); }
            if ((e.key === "Delete" || e.key === "Backspace") && selected) { e.preventDefault(); return deleteSelection(); }
            const step = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] }[e.key];
            if (step && selected && selected.node && nodes.has(selected.node)) {
                e.preventDefault();
                record("nudge:" + selected.node);
                const n = nodes.get(selected.node), d = e.shiftKey ? 5 * SNAP : SNAP;
                n.x += step[0] * d; n.y += step[1] * d;
                place(n); renderEdges(); drawMinimap(); updateDirty();
            }
        });

        function renderAll() {
            renderHeader();
            renderNodes();
            renderEdges();
            renderPanel();
            drawMinimap();
            updateDirty();
        }

        new ResizeObserver(() => (fitWhenShown ? fit() : apply())).observe(viewport);

        // ---- Load ---------------------------------------------------------------------------------
        async function load() {
            try {
                const getJson = async (url) => {
                    const res = await fetch(url, { headers: { Accept: "application/json" } });
                    if (!res.ok) throw new Error((await res.text()) || res.statusText);
                    return res.json();
                };
                const [nt, detail] = await Promise.all([
                    getJson(api + "/node-types"),
                    definitionId ? getJson(api + "/definitions/" + definitionId) : null,
                ]);
                types = nt.node_types || [];
                if (detail) fromDefinition(detail.definition);
                saved = JSON.stringify(toDefinition());
            } catch (err) {
                root.classList.add("is-ready");
                return flash("Couldn't load the builder: " + err.message);
            }
            renderPalette();
            renderNodes();
            tidy(); // positions aren't stored: lay out by the dependencies
            renderAll();
            root.classList.add("is-ready");
            fit();
            if (document.fonts) document.fonts.ready.then(() => { renderEdges(); drawMinimap(); });
        }
        load();
    }
})();
