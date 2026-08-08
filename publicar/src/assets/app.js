import { getIndexType } from "./types.js";
import * as maplibregl from "https://unpkg.com/maplibre-gl@6.0.0/dist/maplibre-gl.mjs";

const DISCOVER_PATH = "./data/descubrir.json";
const ARCHIVE_PATH = "./data/archivar.json";
const ARCHIVE_PROXY_BASE = "https://geodatos-archive-proxy.josemauricioforonda.workers.dev/geodata";
const RAW_GITHUB_BASE = "https://raw.githubusercontent.com/mauforonda/geodatos/master";
const PAGE_SIZE = 60;
const GEOJSON_MAP_LIMIT = 5 * 1024 * 1024;
const VIEW_STORAGE_KEY = "geodatosbolivia_vista";
const CARTO_TILES = [
  "https://a.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png",
  "https://b.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png",
  "https://c.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png",
  "https://d.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png",
];
const CARTO_LABEL_TILES = CARTO_TILES.map((url) => url.replace("light_nolabels", "light_only_labels"));
const CARTO_DARK_TILES = CARTO_TILES.map((url) => url.replace("light_nolabels", "dark_nolabels"));
const CARTO_DARK_LABEL_TILES = CARTO_TILES.map((url) => url.replace("light_nolabels", "dark_only_labels"));

const state = {
  mode: "archivo",
  query: "",
  limit: PAGE_SIZE,
  expandedCard: null,
  openMaps: new Set(),
  datasets: {
    descubrir: null,
    archivo: null,
  },
  loadPromises: {},
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

function archiveGeojsonProxyUrl(item) {
  return `${ARCHIVE_PROXY_BASE}/${encodeURIComponent(item)}`;
}

function slug(text) {
  return (text || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^0-9A-Za-z]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "")
    .toLowerCase();
}

function archivePreviewUrl({ archiveItem, geoserver, nombre, current, hasMap }) {
  if (!hasMap || !archiveItem) return null;
  const path = current
    ? `archivar/publicados/${slug(geoserver)}/${slug(nombre)}/map.png`
    : `archivar/geobolivia_2021/publicados/${slug(nombre)}/map.png`;
  return `${RAW_GITHUB_BASE}/${path}`;
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
    const flags = row[6] || [0, 0, 0];
    const sample = Array.isArray(row[7]) ? row[7] : [];
    const geojsonBytes = Number(row[8] || 0);
    const attrNames = sample.map((entry) => String(entry[0] || ""));
    const hasMap = Boolean(flags[0]);
    const current = Boolean(flags[2]);
    const geojsonFilename = current ? "dataset.geojson" : `${slug(nombre)}.geojson`;
    const geojsonUrl = archiveItem ? archiveDownloadUrl(archiveItem, geojsonFilename) : null;
    const geoparquetUrl = current && archiveItem
      ? archiveDownloadUrl(archiveItem, "dataset.geoparquet")
      : null;

    return {
      sourceId: source.id,
      geoserver: source.geoserver,
      fuente: source.fuente,
      nombre,
      titulo,
      descripcion,
      fechaArchivado,
      archiveItem,
      previewUrl: archivePreviewUrl({ archiveItem, geoserver: source.geoserver, nombre, current, hasMap }),
      geoparquetUrl,
      geojsonUrl,
      geojsonBytes,
      canShowMap: Boolean(geojsonUrl && geojsonBytes > 0 && geojsonBytes <= GEOJSON_MAP_LIMIT),
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

function collectCoordinates(value, bounds) {
  if (!Array.isArray(value)) return;
  if (value.length >= 2 && value.every((entry) => typeof entry === "number")) {
    bounds[0] = Math.min(bounds[0], value[0]);
    bounds[1] = Math.min(bounds[1], value[1]);
    bounds[2] = Math.max(bounds[2], value[0]);
    bounds[3] = Math.max(bounds[3], value[1]);
    return;
  }
  for (const child of value) collectCoordinates(child, bounds);
}

function geojsonBounds(data) {
  const bounds = [Infinity, Infinity, -Infinity, -Infinity];
  const features = data.type === "FeatureCollection" ? data.features : [data];
  for (const feature of features) {
    if (feature?.geometry?.coordinates) collectCoordinates(feature.geometry.coordinates, bounds);
  }
  return Number.isFinite(bounds[0]) ? bounds : [-69.7, -22.9, -57.4, -9.6];
}

function dominantGeometryType(data) {
  const counts = { polygon: 0, line: 0, point: 0 };
  const features = data.type === "FeatureCollection" ? data.features : [data];
  for (const feature of features) {
    const type = feature?.geometry?.type || "";
    if (type.includes("Polygon")) counts.polygon += 1;
    else if (type.includes("LineString")) counts.line += 1;
    else if (type.includes("Point")) counts.point += 1;
  }
  return Object.entries(counts).sort((a, b) => b[1] - a[1])[0][0];
}

function createMapDataLayer(data) {
  const type = dominantGeometryType(data);
  if (type === "polygon") {
    return {
      type: "fill",
      paint: {
        "fill-color": "#6a9fcc",
        "fill-opacity": 0.42,
        "fill-outline-color": "#355f86",
      },
    };
  }
  if (type === "line") {
    return {
      type: "line",
      paint: { "line-color": "#355f86", "line-width": 2, "line-opacity": 0.8 },
    };
  }
  return {
    type: "circle",
    paint: {
      "circle-color": "#6a9fcc",
      "circle-radius": 5,
      "circle-stroke-color": "#355f86",
      "circle-stroke-width": 1,
    },
  };
}

function closeMapView(mapState) {
  if (!mapState) return;
  mapState.closed = true;
  hideFeatureSheet(mapState);
  mapState.map?.remove();
  mapState.view.replaceWith(mapState.card);
  mapState.card.classList.remove("is-expanded");
  mapState.card.setAttribute("aria-expanded", "false");
  state.openMaps.delete(mapState);
}

function hideFeatureSheet(mapState) {
  if (!mapState.sheet) return;
  mapState.sheet.remove();
  mapState.sheet = null;
}

function showFeatureSheet(mapState, properties) {
  hideFeatureSheet(mapState);

  const sheet = document.createElement("div");
  sheet.className = "map-attributes-sheet";

  const handle = document.createElement("div");
  handle.className = "map-sheet-handle";
  handle.setAttribute("aria-label", "Arrastrar atributos");
  handle.addEventListener("pointerdown", (event) => {
    event.preventDefault();
    event.stopPropagation();
    mapState.sheetAnimation?.cancel();
    mapState.sheetAnimation = null;
    sheet.classList.add("is-dragging");
    const startY = event.clientY;
    const startHeight = sheet.offsetHeight;
    const minHeight = handle.offsetHeight + 4;
    const maxHeight = mapState.view.clientHeight;

    const move = (moveEvent) => {
      const nextHeight = Math.max(
        minHeight,
        Math.min(maxHeight, startHeight + startY - moveEvent.clientY),
      );
      mapState.sheetHeight = nextHeight;
      sheet.style.height = `${nextHeight}px`;
    };
    const end = () => {
      sheet.classList.remove("is-dragging");
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", end);
      window.removeEventListener("pointercancel", end);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", end, { once: true });
    window.addEventListener("pointercancel", end, { once: true });
  });

  const attributes = createAttributeSection(Object.entries(properties || {}), "", false);
  sheet.append(handle);
  if (attributes) sheet.append(attributes);
  mapState.view.append(sheet);
  mapState.sheet = sheet;
  const initialHeight = mapState.sheetHeight || mapState.view.clientHeight / 3;
  mapState.sheetHeight = initialHeight;
  sheet.style.height = `${initialHeight}px`;

  if (!mapState.hasShownSheet) {
    mapState.hasShownSheet = true;
    sheet.style.height = "0px";
    requestAnimationFrame(() => {
      const animation = sheet.animate(
        [{ height: "0px" }, { height: `${initialHeight}px` }],
        { duration: 180, easing: "ease-out", fill: "forwards" },
      );
      mapState.sheetAnimation = animation;
      animation.finished
        .then(() => {
          animation.cancel();
          mapState.sheetAnimation = null;
          sheet.style.height = `${mapState.sheetHeight}px`;
        })
        .catch(() => {});
    });
  }
}

function bindFeatureInteraction(mapState) {
  mapState.map.on("click", "data", (event) => {
    const feature = event.features?.[0];
    if (feature) showFeatureSheet(mapState, feature.properties);
  });
  mapState.map.on("mouseenter", "data", () => {
    mapState.map.getCanvas().style.cursor = "pointer";
  });
  mapState.map.on("mouseleave", "data", () => {
    mapState.map.getCanvas().style.cursor = "";
  });
}

function createMapView(item, card) {
  const view = document.createElement("div");
  view.className = "map-view";

  const canvas = document.createElement("div");
  canvas.className = "map-canvas";
  canvas.setAttribute("aria-label", `Mapa de ${item.titulo}`);

  const mapState = {
    card,
    view,
    map: null,
    sheet: null,
    sheetHeight: null,
    hasShownSheet: false,
    closed: false,
  };
  const close = document.createElement("button");
  close.className = "map-close";
  close.type = "button";
  close.setAttribute("aria-label", "Cerrar mapa");
  close.textContent = "×";
  close.addEventListener("click", () => closeMapView(mapState));

  const message = document.createElement("div");
  message.className = "map-message";
  message.textContent = "Cargando mapa…";

  view.append(canvas, close, message);
  card.replaceWith(view);
  state.openMaps.add(mapState);
  return { mapState, canvas, message };
}

async function openArchiveMap(item, card) {
  const { mapState, canvas, message } = createMapView(item, card);

  try {
    const response = await fetch(archiveGeojsonProxyUrl(item.archiveItem));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (mapState.closed) return;

    const dark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    const baseTiles = dark ? CARTO_DARK_TILES : CARTO_TILES;
    const labelTiles = dark ? CARTO_DARK_LABEL_TILES : CARTO_LABEL_TILES;
    const map = new maplibregl.Map({
      container: canvas,
      center: [-64, -17],
      zoom: 4,
      style: {
        version: 8,
        sources: {
          "carto-light": {
            type: "raster",
            tiles: baseTiles,
            tileSize: 256,
          },
          data: { type: "geojson", data },
          "carto-labels": {
            type: "raster",
            tiles: labelTiles,
            tileSize: 256,
          },
        },
        layers: [
          { id: "carto-light", type: "raster", source: "carto-light" },
          { id: "data", source: "data", ...createMapDataLayer(data) },
          { id: "carto-labels", type: "raster", source: "carto-labels" },
        ],
      },
      attributionControl: false,
    });
    mapState.map = map;
    bindFeatureInteraction(mapState);
    map.on("load", () => {
      message.remove();
      map.fitBounds(geojsonBounds(data), { padding: 28, maxZoom: 14, duration: 0 });
    });
  } catch (error) {
    message.textContent = "No se pudo cargar el mapa.";
    console.error("Error cargando GeoJSON archivado", error);
  }
}

function closeAllMaps() {
  for (const mapState of [...state.openMaps]) closeMapView(mapState);
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

function createMapButton(item, card) {
  if (!item.canShowMap) return null;
  const button = document.createElement("button");
  button.className = "button map-button";
  button.type = "button";
  button.textContent = "Ver mapa";
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    openArchiveMap(item, card);
  });
  return button;
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

function createAttributeSection(sample, title = "Muestra de Atributos", showTitle = true) {
  if (!sample.length) return null;
  const section = document.createElement("section");
  section.className = "attributes";

  if (showTitle) {
    const heading = document.createElement("div");
    heading.className = "attributes-title";
    heading.textContent = title;
    section.append(heading);
  }

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

  const mapButton = createMapButton(item, node);
  if (mapButton) node.querySelector(".details header").append(mapButton);

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

  closeAllMaps();
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
  if (Array.isArray(state.datasets[state.mode])) {
    renderDataset(state.mode);
    return;
  }
  renderLoading(state.mode);
}

function renderLoading(mode) {
  closeExpandedCard();
  els.results.replaceChildren();
  els.results.classList.add("is-hidden");
  els.emptyState.classList.add("is-hidden");
  els.placeholderState.classList.remove("is-hidden");
  els.placeholderState.querySelector("h2").textContent = "Cargando índice…";
  els.placeholderState.querySelector("p").textContent = mode === "archivo"
    ? "Preparando los conjuntos archivados."
    : "Preparando los conjuntos descubiertos.";
  els.resultsSummary.textContent = "";
  els.footer.classList.add("is-hidden");
  els.loadMore.classList.add("is-hidden");
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
      loadMode(state.mode);
    });
  }

  document.addEventListener("click", (event) => {
    if (!state.expandedCard) return;
    if (event.target.closest(".result")) return;
    closeExpandedCard();
  });

  window.addEventListener("hashchange", () => {
    const nextMode = window.location.hash.replace("#", "");
    if (nextMode === "archivo" || nextMode === "descubrir") {
      state.mode = nextMode;
      localStorage.setItem(VIEW_STORAGE_KEY, state.mode);
      render();
      loadMode(state.mode);
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

  loadMode(state.mode);
}

async function loadMode(mode) {
  if (Array.isArray(state.datasets[mode])) {
    if (state.mode === mode) render();
    return state.datasets[mode];
  }
  if (state.loadPromises[mode]) return state.loadPromises[mode];

  render();
  const loader = mode === "archivo" ? loadArchive : loadDiscover;
  const promise = loader().then((items) => {
    state.datasets[mode] = items;
    if (state.mode === mode) render();
    return items;
  }).catch((error) => {
    console.error(error);
    if (state.mode === mode) {
      els.results.replaceChildren();
      els.results.classList.add("is-hidden");
      els.emptyState.classList.remove("is-hidden");
      els.emptyState.querySelector("h2").textContent = "No se pudo cargar el índice";
      els.emptyState.querySelector("p").textContent = error.message;
      els.resultsSummary.textContent = "Error cargando datos.";
      els.footer.classList.remove("is-hidden");
    }
    throw error;
  }).finally(() => {
    delete state.loadPromises[mode];
  });
  state.loadPromises[mode] = promise;
  return promise;
}

bootstrap();
