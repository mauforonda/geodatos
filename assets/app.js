import { getIndexType } from "./types.js";

const DISCOVER_PATH = "./data/descubrir.json";
const ARCHIVE_PATH = "./data/archivar.json";
const PAGE_SIZE = 60;
const VIEW_STORAGE_KEY = "geodatosbolivia_vista";

const state = {
  mode: "archivo",
  query: "",
  limit: PAGE_SIZE,
  expandedCard: null,
  datasets: {
    descubrir: [],
    archivo: [],
  },
};

const els = {
  modeButtons: Array.from(document.querySelectorAll(".nav > .buttons > .button")),
  heroSiteTitle: document.querySelector("#site-title"),
  heroShort: document.querySelector("#legend"),
  searchInput: document.querySelector("#search-input"),
  resultsSummary: document.querySelector("#results-summary"),
  results: document.querySelector("#results"),
  emptyState: document.querySelector("#empty-state"),
  placeholderState: document.querySelector("#placeholder-state"),
  footer: document.querySelector(".footer"),
  loadMore: document.querySelector("#load-more"),
  template: document.querySelector("#discover-card-template"),
};

const imageObserver = new IntersectionObserver(
  (entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) continue;
      const img = entry.target;
      const src = img.dataset.src;
      if (src) img.src = src;
      imageObserver.unobserve(img);
    }
  },
  { rootMargin: "300px 0px" },
);

const desktopHoverQuery = window.matchMedia("(hover: hover) and (pointer: fine)");
const narrowLayoutQuery = window.matchMedia("(max-width: 860px)");

function usesDesktopHover() {
  return desktopHoverQuery.matches && !narrowLayoutQuery.matches;
}

function enableLiveReload() {
  const host = window.location.hostname;
  if (!(host === "127.0.0.1" || host === "localhost")) return;

  try {
    const source = new EventSource("/__reload");
    source.addEventListener("reload", () => {
      window.location.reload();
    });
  } catch (error) {
    console.debug("live reload unavailable", error);
  }
}

function normalize(text) {
  return (text || "")
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function inferBaseWms(ows) {
  if (ows.endsWith("/ows")) {
    return `${ows.slice(0, -4)}/wms`;
  }
  return null;
}

function reflectPreviewUrl(ows, nombre) {
  const baseWms = inferBaseWms(ows);
  if (!baseWms) return null;
  return `${baseWms}/reflect?layers=${nombre}&format=image/jpeg&transparent=true&width=540&height=420`;
}

function archiveDownloadUrl(item, filename) {
  return `https://archive.org/download/${encodeURIComponent(item)}/${encodeURIComponent(filename)}`;
}

function buildWmsLink(ows, nombre, format) {
  const baseWms = inferBaseWms(ows);
  if (baseWms) {
    const formatPart = format ? `&format=${format}` : "";
    return `${baseWms}/reflect?layers=${nombre}${formatPart}`;
  }
  return `${ows}?service=wms&request=GetMap&layers=${nombre}&format=${format}`;
}

function buildWfsLink(ows, nombre, format, extra = "") {
  const suffix = extra ? `&${extra}` : "";
  return `${ows}?service=wfs&request=GetFeature&typeName=${nombre}&outputFormat=${format}${suffix}`;
}

function decodeDiscover(payload) {
  const sources = payload.s.map((source, index) => ({
    id: index,
    geoserver: source[0],
    fuente: source[1] || source[0],
    ows: source[2],
  }));

  return payload.r.map((row) => {
    const source = sources[row[0]];
    const nombre = row[1];
    const titulo = row[2] || nombre;
    const descripcion = row[3] || "";
    const fechaEncontrado = row[4] || "";
    const flags = row[5] || [0, 0];
    const wms = Boolean(flags[0]);
    const wfs = Boolean(flags[1]);
    const previewUrl = wms ? reflectPreviewUrl(source.ows, nombre) : null;

    return {
      sourceId: source.id,
      geoserver: source.geoserver,
      fuente: source.fuente,
      ows: source.ows,
      nombre,
      titulo,
      descripcion,
      fechaEncontrado,
      wms,
      wfs,
      previewUrl,
      mapUrl: wms ? buildWmsLink(source.ows, nombre, "application/openlayers") : null,
      exampleUrl: wfs ? buildWfsLink(source.ows, nombre, "application/json", "count=1") : null,
      geojsonUrl: wfs ? buildWfsLink(source.ows, nombre, "application/json") : null,
      shpUrl: wfs ? buildWfsLink(source.ows, nombre, "SHAPE-ZIP") : null,
      csvUrl: wfs ? buildWfsLink(source.ows, nombre, "csv") : null,
      kmlUrl: wms ? buildWmsLink(source.ows, nombre, "application/vnd.google-earth.kml") : null,
      geotiffUrl: wms ? buildWmsLink(source.ows, nombre, "image/geotiff") : null,
      searchText: normalize([source.geoserver, source.fuente, nombre, titulo, descripcion].join(" ")),
    };
  });
}

async function loadDiscover() {
  const response = await fetch(DISCOVER_PATH);
  if (!response.ok) {
    throw new Error(`No se pudo cargar ${DISCOVER_PATH}`);
  }
  const payload = await response.json();
  return decodeDiscover(payload);
}

function decodeArchive(payload) {
  const sources = payload.s.map((source, index) => ({
    id: index,
    geoserver: source[0],
    fuente: source[1] || source[0],
  }));

  return payload.r.map((row) => {
    const source = sources[row[0]];
    const nombre = row[1];
    const titulo = row[2] || nombre;
    const descripcion = row[3] || "";
    const fechaArchivado = row[4] || "";
    const archiveItem = row[5];
    const previewUrl = row[6] || "";
    const geojsonUrl = row[7] || "";
    const geoparquetUrl = row[8] || "";
    const flags = row[9] || [0, 0];
    const sample = Array.isArray(row[10]) ? row[10] : [];
    const attrNames = sample.map((entry) => String(entry[0] || ""));
    const hasMap = Boolean(flags[0]);

    return {
      sourceId: source.id,
      geoserver: source.geoserver,
      fuente: source.fuente,
      nombre,
      titulo,
      descripcion,
      fechaArchivado,
      archiveItem,
      previewUrl:
        hasMap && (previewUrl || archiveItem)
          ? previewUrl || archiveDownloadUrl(archiveItem, "cover.png")
          : null,
      geoparquetUrl: geoparquetUrl || null,
      geojsonUrl: geojsonUrl || null,
      sample,
      searchText: normalize([source.fuente, nombre, titulo, descripcion, attrNames.join(" ")].join(" ")),
    };
  });
}

async function loadArchive() {
  const response = await fetch(ARCHIVE_PATH);
  if (!response.ok) {
    throw new Error(`No se pudo cargar ${ARCHIVE_PATH}`);
  }
  const payload = await response.json();
  return decodeArchive(payload);
}

function formatDate(text) {
  if (!text) return "";
  const date = new Date(`${text}T00:00:00Z`);
  return new Intl.DateTimeFormat("es-BO", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  }).format(date);
}

function filterItems(items) {
  const q = normalize(state.query);
  return items.filter((item) => {
    if (q && !item.searchText.includes(q)) return false;
    return true;
  });
}

function createChip(label, href) {
  if (!href) return null;
  const a = document.createElement("a");
  a.className = "button";
  a.href = href;
  a.target = "_blank";
  a.rel = "noreferrer";
  a.textContent = label;
  return a;
}

function createChipGroup(title, chips) {
  if (chips.length === 0) return null;
  const wrap = document.createElement("section");
  wrap.className = "button-group";

  const heading = document.createElement("div");
  heading.className = "button-title";
  heading.textContent = title;
  wrap.append(heading);

  const row = document.createElement("div");
  row.className = "button-row";
  row.append(...chips);
  wrap.append(row);
  return wrap;
}

function createAttributeSection(sample) {
  if (!sample.length) return null;
  const section = document.createElement("section");
  section.className = "attributes";

  const heading = document.createElement("div");
  heading.className = "attributes-title";
  heading.textContent = "Muestra de Atributos";
  section.append(heading);

  const list = document.createElement("div");
  list.className = "attribute-table";
  for (const [key, value] of sample) {
    const row = document.createElement("div");
    row.className = "attribute";

    const keyEl = document.createElement("div");
    keyEl.className = "key";
    keyEl.textContent = key;

    const valueEl = document.createElement("div");
    valueEl.className = "value";
    valueEl.textContent = value === null ? "null" : String(value);

    row.append(keyEl, valueEl);
    list.append(row);
  }
  section.append(list);
  return section;
}

function openCard(node) {
  if (state.expandedCard && state.expandedCard !== node) {
    state.expandedCard.classList.remove("is-expanded");
    state.expandedCard.setAttribute("aria-expanded", "false");
  }
  state.expandedCard = node;
  node.classList.add("is-expanded");
  node.setAttribute("aria-expanded", "true");
}

function closeExpandedCard() {
  if (!state.expandedCard) return;
  state.expandedCard.classList.remove("is-expanded");
  state.expandedCard.setAttribute("aria-expanded", "false");
  state.expandedCard = null;
}

function bindCardInteractions(node) {
  node.addEventListener("mouseenter", () => openCard(node));
  node.addEventListener("mouseleave", () => {
    if (!usesDesktopHover()) return;
    if (node.matches(":focus-within")) return;
    if (state.expandedCard === node) {
      closeExpandedCard();
    }
  });
  node.addEventListener("focusin", () => openCard(node));
  node.addEventListener("focusout", () => {
    if (!usesDesktopHover()) return;
    queueMicrotask(() => {
      if (!node.matches(":focus-within") && state.expandedCard === node) {
        closeExpandedCard();
      }
    });
  });
  node.addEventListener("click", (event) => {
    const clickedLink = event.target.closest("a");
    if (clickedLink) return;
    if (usesDesktopHover()) return;
    if (state.expandedCard !== node) {
      openCard(node);
    }
  });
  node.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openCard(node);
    }
    if (event.key === "Escape") {
      closeExpandedCard();
      node.blur();
    }
  });
}

function renderDiscoverCard(item) {
  const node = els.template.content.firstElementChild.cloneNode(true);
  const thumb = node.querySelector(".image");
  const fallback = node.querySelector(".fallback");
  const linksWrap = node.querySelector(".buttons");

  node.dataset.id = `${item.geoserver}/${item.nombre}`;
  node.setAttribute("aria-expanded", "false");
  node.querySelector(".result-title .title").textContent = item.titulo;
  node.querySelector(".details .title").textContent = item.titulo;
  node.querySelector(".geoserver").textContent = item.fuente;

  const description = node.querySelector(".description");
  if (item.descripcion) {
    description.textContent = item.descripcion;
    description.classList.remove("is-hidden");
  }

  if (item.previewUrl) {
    thumb.dataset.src = item.previewUrl;
    thumb.alt = "";
    thumb.classList.remove("is-hidden");
    thumb.addEventListener(
      "load",
      () => {
        fallback.classList.add("is-hidden");
      },
      { once: true },
    );
    thumb.addEventListener(
      "error",
      () => {
        thumb.classList.add("is-hidden");
        fallback.classList.remove("is-hidden");
      },
      { once: true },
    );
    imageObserver.observe(thumb);
  } else {
    fallback.textContent = "sin vista previa";
  }

  const groups = [
    createChipGroup("Vistas", [
      createChip("ver mapa", item.mapUrl),
      createChip("ver ejemplo", item.exampleUrl),
    ].filter(Boolean)),
    createChipGroup("Vectores", [
      createChip("GeoJSON", item.geojsonUrl),
      createChip("Shapefile", item.shpUrl),
      createChip("CSV", item.csvUrl),
    ].filter(Boolean)),
    createChipGroup("Rasters", [
      createChip("KML", item.kmlUrl),
      createChip("GeoTIFF", item.geotiffUrl),
    ].filter(Boolean)),
  ].filter(Boolean);

  if (groups.length > 0) {
    linksWrap.append(...groups);
  }

  if (item.fechaEncontrado) {
    const discovered = document.createElement("p");
    discovered.className = "date";
    discovered.textContent = `descubierto el ${formatDate(item.fechaEncontrado)}`;
    node.querySelector(".details-body").append(discovered);
  }

  bindCardInteractions(node);
  return node;
}

function renderArchiveCard(item) {
  const node = els.template.content.firstElementChild.cloneNode(true);
  const thumb = node.querySelector(".image");
  const fallback = node.querySelector(".fallback");
  const linksWrap = node.querySelector(".buttons");

  node.dataset.id = item.archiveItem;
  node.setAttribute("aria-expanded", "false");
  node.querySelector(".result-title .title").textContent = item.titulo;
  node.querySelector(".details .title").textContent = item.titulo;
  node.querySelector(".geoserver").textContent = item.fuente;

  const description = node.querySelector(".description");
  if (item.descripcion) {
    description.textContent = item.descripcion;
    description.classList.remove("is-hidden");
  }

  if (item.previewUrl) {
    thumb.dataset.src = item.previewUrl;
    thumb.alt = "";
    thumb.classList.remove("is-hidden");
    thumb.addEventListener(
      "load",
      () => {
        fallback.classList.add("is-hidden");
      },
      { once: true },
    );
    thumb.addEventListener(
      "error",
      () => {
        thumb.classList.add("is-hidden");
        fallback.classList.remove("is-hidden");
      },
      { once: true },
    );
    imageObserver.observe(thumb);
  } else {
    fallback.textContent = "";
  }

  const groups = [
    createChipGroup("Descargas", [
      createChip("GeoParquet", item.geoparquetUrl),
      createChip("GeoJSON", item.geojsonUrl),
    ].filter(Boolean)),
  ].filter(Boolean);

  if (groups.length > 0) {
    linksWrap.append(...groups);
  }

  const attrs = createAttributeSection(item.sample);
  if (attrs) {
    node.querySelector(".details-body").append(attrs);
  }

  if (item.fechaArchivado) {
    const archived = document.createElement("p");
    archived.className = "date";
    archived.textContent = `archivado el ${formatDate(item.fechaArchivado)}`;
    node.querySelector(".details-body").append(archived);
  }

  bindCardInteractions(node);
  return node;
}

function renderDataset(mode) {
  const items = state.datasets[mode] || [];
  const filtered = filterItems(items);
  const visible = filtered.slice(0, state.limit);
  const renderer = mode === "archivo" ? renderArchiveCard : renderDiscoverCard;

  closeExpandedCard();
  els.results.replaceChildren(...visible.map(renderer));
  els.emptyState.classList.toggle("is-hidden", filtered.length !== 0);
  els.placeholderState.classList.add("is-hidden");
  els.results.classList.toggle("is-hidden", filtered.length === 0);

  els.resultsSummary.textContent = `${filtered.length.toLocaleString("en-US")} conjuntos de datos`;
  els.footer.classList.remove("is-hidden");
  const hiddenCount = filtered.length - visible.length;
  els.loadMore.classList.toggle("is-hidden", hiddenCount <= 0);
  els.loadMore.textContent = hiddenCount > 0 ? `Mostrar ${Math.min(PAGE_SIZE, hiddenCount)} más` : "Mostrar más";
}

function renderArchivePlaceholder() {
  closeExpandedCard();
  els.results.replaceChildren();
  els.results.classList.add("is-hidden");
  els.emptyState.classList.add("is-hidden");
  els.placeholderState.classList.remove("is-hidden");
  els.resultsSummary.textContent = "Índice de archivados en preparación.";
  els.footer.classList.remove("is-hidden");
  els.loadMore.classList.add("is-hidden");
}

function renderShell() {
  const indexType = getIndexType(state.mode);

  els.heroSiteTitle.textContent = "GeoDatos sobre Bolivia";
  els.heroShort.textContent = indexType.descripcionCorta;
  els.searchInput.placeholder = indexType.busquedaPlaceholder || "";

  for (const button of els.modeButtons) {
    const buttonType = getIndexType(button.dataset.mode);
    const active = button.dataset.mode === state.mode;
    button.textContent = buttonType.nombre;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  }
}

function render() {
  renderShell();
  if (state.datasets[state.mode]?.length) {
    renderDataset(state.mode);
    return;
  }
  renderArchivePlaceholder();
}

function resetPagination() {
  state.limit = PAGE_SIZE;
}

function bindEvents() {
  els.searchInput.addEventListener("input", (event) => {
    state.query = event.target.value;
    resetPagination();
    render();
  });

  els.loadMore.addEventListener("click", () => {
    state.limit += PAGE_SIZE;
    render();
  });

  for (const button of els.modeButtons) {
    button.addEventListener("click", () => {
      state.mode = button.dataset.mode;
      localStorage.setItem(VIEW_STORAGE_KEY, state.mode);
      window.location.hash = state.mode;
      render();
    });
  }

  document.addEventListener("click", (event) => {
    if (!state.expandedCard) return;
    if (event.target.closest(".card")) return;
    closeExpandedCard();
  });

  window.addEventListener("hashchange", () => {
    const nextMode = window.location.hash.replace("#", "");
    if (nextMode === "archivo" || nextMode === "descubrir") {
      state.mode = nextMode;
      localStorage.setItem(VIEW_STORAGE_KEY, state.mode);
      render();
    }
  });
}

async function bootstrap() {
  enableLiveReload();
  bindEvents();

  const hashMode = window.location.hash.replace("#", "");
  const storedMode = localStorage.getItem(VIEW_STORAGE_KEY);
  if (hashMode === "archivo" || hashMode === "descubrir") {
    state.mode = hashMode;
  } else if (storedMode === "archivo" || storedMode === "descubrir") {
    state.mode = storedMode;
  }
  localStorage.setItem(VIEW_STORAGE_KEY, state.mode);
  renderShell();

  try {
    const [discoverItems, archiveItems] = await Promise.all([loadDiscover(), loadArchive()]);
    state.datasets.descubrir = discoverItems;
    state.datasets.archivo = archiveItems;
    render();
  } catch (error) {
    console.error(error);
    els.results.replaceChildren();
    els.results.classList.add("is-hidden");
    els.emptyState.classList.remove("is-hidden");
    els.emptyState.querySelector("h2").textContent = "No se pudo cargar el índice";
    els.emptyState.querySelector("p").textContent = error.message;
    els.resultsSummary.textContent = "Error cargando datos.";
    els.footer.classList.remove("is-hidden");
  }
}

bootstrap();
