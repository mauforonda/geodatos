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
const MAP_CONTINUOUS_COLORS = ["#f3e4bf", "#f5c1ac", "#e9a1b0", "#c48cc0", "#7f83c7"];
const MAP_DARK_CONTINUOUS_COLORS = ["#c8bb9d", "#cb9e89", "#c7808a", "#ae6b9b", "#7863ac"];
const MAP_CATEGORICAL_COLORS = [
  "#8dd3c7",
  "#ffffb3",
  "#bebada",
  "#fb8072",
  "#80b1d3",
  "#fdb462",
  "#b3de69",
  "#fccde5",
  "#d9d9d9",
];
const MAP_INVALID_COLOR = "#c4c8cb";
const resultResizeAnimations = new WeakMap();
const darkModeQuery = window.matchMedia("(prefers-color-scheme: dark)");

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

function geojsonDatasetName(url) {
  if (!url) return "dataset";
  try {
    const filename = decodeURIComponent(new URL(url).pathname.split("/").pop() || "");
    return filename.replace(/\.geojson$/i, "") || "dataset";
  } catch {
    return "dataset";
  }
}

function archiveGeojsonProxyUrl(item, geojsonUrl) {
  const base = `${ARCHIVE_PROXY_BASE}/${encodeURIComponent(item)}`;
  const dataset = geojsonDatasetName(geojsonUrl);
  return dataset === "dataset" ? base : `${base}/${encodeURIComponent(dataset)}`;
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
  if (!response.ok) throw new Error(`No se pudo cargar ${DISCOVER_PATH}`);
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
    const flags = row[6] || [0, 0];
    const sample = Array.isArray(row[7]) ? row[7] : [];
    const geojsonBytes = Number(row[8] || 0);
    const geoparquetBytes = Number(row[9] || 0);
    const epsg = Number(row[10] || 4326);
    const catalogGeojsonUrl = row[11] || null;
    const catalogGeoparquetUrl = row[12] || null;
    const attrNames = sample.map((entry) => String(entry[0] || ""));
    const hasMap = Boolean(flags[0]);
    const current = Boolean(flags[1]);
    const geojsonFilename = current ? "dataset.geojson" : `${slug(nombre)}.geojson`;
    const geojsonUrl = catalogGeojsonUrl || (archiveItem
      ? archiveDownloadUrl(archiveItem, geojsonFilename)
      : null);
    const geoparquetUrl = catalogGeoparquetUrl || (current && archiveItem
      ? archiveDownloadUrl(archiveItem, "dataset.geoparquet")
      : null);

    return {
      sourceId: source.id,
      geoserver: source.geoserver,
      fuente: source.fuente,
      nombre,
      titulo,
      descripcion,
      fechaArchivado,
      archiveItem,
      epsg,
      previewUrl: archivePreviewUrl({ archiveItem, geoserver: source.geoserver, nombre, current, hasMap }),
      geoparquetUrl,
      geojsonUrl,
      geojsonBytes,
      geoparquetBytes,
      canShowMap: Boolean(geojsonUrl && geojsonBytes > 0 && geojsonBytes <= GEOJSON_MAP_LIMIT),
      sample,
      searchText: normalize([source.fuente, nombre, titulo, descripcion, attrNames.join(" ")].join(" ")),
    };
  });
}

async function loadArchive() {
  const response = await fetch(ARCHIVE_PATH);
  if (!response.ok) throw new Error(`No se pudo cargar ${ARCHIVE_PATH}`);
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

function geojsonEpsg(data) {
  const crsName = data?.crs?.properties?.name || data?.crs?.properties?.href || "";
  const match = String(crsName).match(/(?:epsg(?::|[/]{1,2})|::)(\d+)/i);
  return match ? Number(match[1]) : 4326;
}

function utmToWgs84([x, y, ...rest], zone, southernHemisphere) {
  const a = 6378137;
  const eccentricitySquared = 0.00669438;
  const eccentricityPrimeSquared = eccentricitySquared / (1 - eccentricitySquared);
  const scale = 0.9996;
  const e1 = (1 - Math.sqrt(1 - eccentricitySquared)) /
    (1 + Math.sqrt(1 - eccentricitySquared));
  const longitudeOrigin = (zone - 1) * 6 - 180 + 3;

  let easting = x - 500000;
  let northing = y;
  if (southernHemisphere) northing -= 10000000;

  const meridionalArc = northing / scale;
  const mu = meridionalArc /
    (a * (1 - eccentricitySquared / 4 - 3 * eccentricitySquared ** 2 / 64 -
      5 * eccentricitySquared ** 3 / 256));
  const footprintLatitude = mu +
    (3 * e1 / 2 - 27 * e1 ** 3 / 32) * Math.sin(2 * mu) +
    (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * Math.sin(4 * mu) +
    (151 * e1 ** 3 / 96) * Math.sin(6 * mu) +
    (1097 * e1 ** 4 / 512) * Math.sin(8 * mu);

  const sinLatitude = Math.sin(footprintLatitude);
  const cosLatitude = Math.cos(footprintLatitude);
  const tangent = Math.tan(footprintLatitude);
  const radiusOfCurvature = a /
    Math.sqrt(1 - eccentricitySquared * sinLatitude ** 2);
  const radiusOfMeridian = a * (1 - eccentricitySquared) /
    (1 - eccentricitySquared * sinLatitude ** 2) ** 1.5;
  const tangentSquared = tangent ** 2;
  const latitudeParameter = eccentricityPrimeSquared * cosLatitude ** 2;
  const distance = easting / (radiusOfCurvature * scale);

  const latitude = footprintLatitude - (radiusOfCurvature * tangent / radiusOfMeridian) *
    (distance ** 2 / 2 -
      (5 + 3 * tangentSquared + 10 * latitudeParameter - 4 * latitudeParameter ** 2 -
        9 * eccentricityPrimeSquared) * distance ** 4 / 24 +
      (61 + 90 * tangentSquared + 298 * latitudeParameter + 45 * tangentSquared ** 2 -
        252 * eccentricityPrimeSquared - 3 * latitudeParameter ** 2) * distance ** 6 / 720);
  const longitude = (distance -
    (1 + 2 * tangentSquared + latitudeParameter) * distance ** 3 / 6 +
    (5 - 2 * latitudeParameter + 28 * tangentSquared - 3 * latitudeParameter ** 2 +
      8 * eccentricityPrimeSquared + 24 * tangentSquared ** 2) * distance ** 5 / 120) /
    cosLatitude;

  return [
    (longitudeOrigin + longitude * 180 / Math.PI),
    (latitude * 180 / Math.PI),
    ...rest,
  ];
}

function webMercatorToWgs84([x, y, ...rest]) {
  return [
    x / 6378137 * 180 / Math.PI,
    (2 * Math.atan(Math.exp(y / 6378137)) - Math.PI / 2) * 180 / Math.PI,
    ...rest,
  ];
}

function capeVerdeToWgs84([x, y, ...rest]) {
  const a = 6378137;
  const eccentricity = 0.0818191908426;
  const eccentricitySquared = eccentricity ** 2;
  const latitudeOrigin = 15.8333333333333 * Math.PI / 180;
  const longitudeOrigin = -24 * Math.PI / 180;
  const latitudeOne = 15 * Math.PI / 180;
  const latitudeTwo = 16.6666666666667 * Math.PI / 180;
  const falseEasting = 161587.83;
  const falseNorthing = 128511.202;
  const m = (latitude) => Math.cos(latitude) /
    Math.sqrt(1 - eccentricitySquared * Math.sin(latitude) ** 2);
  const t = (latitude) => Math.tan(Math.PI / 4 - latitude / 2) /
    ((1 - eccentricity * Math.sin(latitude)) /
      (1 + eccentricity * Math.sin(latitude))) ** (eccentricity / 2);
  const n = Math.log(m(latitudeOne) / m(latitudeTwo)) /
    Math.log(t(latitudeOne) / t(latitudeTwo));
  const f = m(latitudeOne) / (n * t(latitudeOne) ** n);
  const rhoOrigin = a * f * t(latitudeOrigin) ** n;
  const adjustedX = x - falseEasting;
  const adjustedY = y - falseNorthing;
  const rho = Math.sign(n) * Math.sqrt(adjustedX ** 2 + (rhoOrigin - adjustedY) ** 2);
  const theta = Math.atan2(adjustedX, rhoOrigin - adjustedY);
  const projectedT = (rho / (a * f)) ** (1 / n);
  let latitude = Math.PI / 2 - 2 * Math.atan(projectedT);
  for (let index = 0; index < 5; index += 1) {
    latitude = Math.PI / 2 - 2 * Math.atan(
      projectedT * ((1 - eccentricity * Math.sin(latitude)) /
        (1 + eccentricity * Math.sin(latitude))) ** (eccentricity / 2),
    );
  }

  return [
    (longitudeOrigin + theta / n) * 180 / Math.PI,
    latitude * 180 / Math.PI,
    ...rest,
  ];
}

function transformGeojson(data, transform) {
  const transformGeometry = (geometry) => {
    if (!geometry) return;
    if (geometry.type === "GeometryCollection") {
      geometry.geometries.forEach(transformGeometry);
    } else if (geometry.coordinates) {
      geometry.coordinates = transformCoordinates(geometry.coordinates, transform);
    }
  };
  const transformCoordinates = (coordinates, coordinateTransform) => {
    if (coordinates.length >= 2 && coordinates.every((value) => typeof value === "number")) {
      return coordinateTransform(coordinates);
    }
    return coordinates.map((child) => transformCoordinates(child, coordinateTransform));
  };

  if (data.type === "FeatureCollection") {
    data.features.forEach((feature) => transformGeometry(feature.geometry));
  } else if (data.type === "Feature") {
    transformGeometry(data.geometry);
  } else {
    transformGeometry(data);
  }
  delete data.crs;
  return data;
}

function prepareMapGeojson(data, sourceEpsg = geojsonEpsg(data)) {
  const epsg = Number(sourceEpsg) || 4326;
  if (epsg === 4326 || epsg === 4674) return data;
  if (epsg === 3857 || epsg === 3395 || epsg === 900913) {
    return transformGeojson(data, webMercatorToWgs84);
  }
  if (epsg === 4826) return transformGeojson(data, capeVerdeToWgs84);

  const utmZone = epsg >= 32601 && epsg <= 32660 ? epsg - 32600 :
    epsg >= 32701 && epsg <= 32760 ? epsg - 32700 :
    epsg === 5355 ? 20 : null;
  if (utmZone) return transformGeojson(data, (coordinate) =>
    utmToWgs84(
      coordinate,
      utmZone,
      epsg >= 32701 && epsg <= 32760 || epsg === 5355,
    ));

  console.warn(`CRS EPSG:${epsg} no soportado; se mostrarán las coordenadas originales.`);
  return data;
}

function geojsonBounds(data) {
  const bounds = [Infinity, Infinity, -Infinity, -Infinity];
  const features = data.type === "FeatureCollection" ? data.features : [data];
  for (const feature of features) {
    if (feature?.geometry?.coordinates) collectCoordinates(feature.geometry.coordinates, bounds);
  }
  return Number.isFinite(bounds[0]) ? bounds : [-69.7, -22.9, -57.4, -9.6];
}

function mapFeatures(data) {
  return data.type === "FeatureCollection" ? data.features : [data];
}

function analyzeMapAttributes(data) {
  const valuesByKey = new Map();
  for (const feature of mapFeatures(data)) {
    for (const [key, value] of Object.entries(feature?.properties || {})) {
      if (!valuesByKey.has(key)) valuesByKey.set(key, []);
      valuesByKey.get(key).push(value);
    }
  }

  const options = new Map();
  for (const [key, values] of valuesByKey) {
    const numbers = values.filter((value) => typeof value === "number" && Number.isFinite(value));
    const uniqueNumbers = [...new Set(numbers)].sort((a, b) => a - b);
    if (uniqueNumbers.length >= 2) {
      options.set(key, {
        key,
        type: "continuous",
        min: uniqueNumbers[0],
        max: uniqueNumbers[uniqueNumbers.length - 1],
        colors: darkModeQuery.matches ? MAP_DARK_CONTINUOUS_COLORS : MAP_CONTINUOUS_COLORS,
      });
      continue;
    }

    const categories = [...new Set(
      values.filter((value) => typeof value === "string" && value.trim()).map((value) => value.trim()),
    )].sort((a, b) => a.localeCompare(b, "es"));
    if (categories.length >= 2 && categories.length <= MAP_CATEGORICAL_COLORS.length) {
      options.set(key, {
        key,
        type: "categorical",
        categories,
        colors: MAP_CATEGORICAL_COLORS.slice(0, categories.length),
      });
    }
  }
  return options;
}

function mapColors() {
  const styles = getComputedStyle(document.documentElement);
  if (darkModeQuery.matches) {
    return {
      fill: "#3e4d79",
      border: styles.getPropertyValue("--frame").trim() || "#1b1f2b",
      highlight: "#3761c7",
    };
  }
  return {
    fill: styles.getPropertyValue("--fill").trim() || "#b8e1f3",
    border: styles.getPropertyValue("--border-strong").trim() || "rgba(45, 53, 64, 0.26)",
    highlight: styles.getPropertyValue("--highlight").trim() || "#5b95c7",
  };
}

function mappingColorExpression(mapping) {
  const value = ["get", mapping.key];
  if (mapping.type === "categorical") {
    return [
      "match",
      value,
      ...mapping.categories.flatMap((category, index) => [category, mapping.colors[index]]),
      MAP_INVALID_COLOR,
    ];
  }
  return [
    "case",
    ["==", ["typeof", value], "number"],
    [
      "interpolate",
      ["linear"],
      value,
      mapping.min,
      mapping.colors[0],
      mapping.max,
      mapping.colors[mapping.colors.length - 1],
    ],
    MAP_INVALID_COLOR,
  ];
}

function mapOpacityExpression(mapped) {
  return mapped
    ? ["case", ["boolean", ["feature-state", "selected"], false], 1, 0.8]
    : 0.8;
}

function mapStrokeOpacityExpression(mapped) {
  return mapped
    ? ["case", ["boolean", ["feature-state", "selected"], false], 1, 0.95]
    : 1;
}

function mapStrokeWidthExpression(mapped) {
  return [
    "case",
    ["boolean", ["feature-state", "selected"], false],
    2,
    mapped
      ? ["case", ["boolean", ["feature-state", "hovered"], false], 2.5, 1]
      : 1,
  ];
}

function mapLineWidthExpression(mapped) {
  return [
    "case",
    ["boolean", ["feature-state", "selected"], false],
    2.5,
    mapped
      ? ["case", ["boolean", ["feature-state", "hovered"], false], 2.5, 1.5]
      : 1.5,
  ];
}

function dominantGeometryType(data) {
  const counts = { polygon: 0, line: 0, point: 0 };
  const features = mapFeatures(data);
  for (const feature of features) {
    const type = feature?.geometry?.type || "";
    if (type.includes("Polygon")) counts.polygon += 1;
    else if (type.includes("LineString")) counts.line += 1;
    else if (type.includes("Point")) counts.point += 1;
  }
  return Object.entries(counts).sort((a, b) => b[1] - a[1])[0][0];
}

function createMapDataLayer(data, mapping = null) {
  const type = dominantGeometryType(data);
  const colors = mapColors();
  const color = mapping ? mappingColorExpression(mapping) : null;
  const fillColor = color || [
    "case",
    ["boolean", ["feature-state", "selected"], false],
    colors.highlight,
    colors.fill,
  ];
  if (type === "polygon") {
    return {
      type: "fill",
      paint: {
        "fill-color": fillColor,
        "fill-opacity": mapOpacityExpression(mapping),
      },
    };
  }
  if (type === "line") {
    return {
      type: "line",
      paint: {
        "line-color": color || fillColor,
        "line-width": mapLineWidthExpression(mapping),
        "line-opacity": mapStrokeOpacityExpression(mapping),
      },
    };
  }
  return {
    type: "circle",
    paint: {
      "circle-color": fillColor,
      "circle-radius": 5,
      "circle-opacity": mapOpacityExpression(mapping),
      "circle-stroke-color": colors.border,
      "circle-stroke-width": mapStrokeWidthExpression(mapping),
      "circle-stroke-opacity": mapStrokeOpacityExpression(mapping),
    },
  };
}

function createMapBorderLayer(data, mapping = null) {
  const colors = mapColors();
  if (dominantGeometryType(data) !== "polygon") return null;
  return {
    id: "data-border",
    type: "line",
    source: "data",
    paint: {
      "line-color": colors.border,
      "line-width": mapStrokeWidthExpression(mapping),
      "line-opacity": mapStrokeOpacityExpression(mapping),
    },
  };
}

function createMapLegend(mapping) {
  const legend = document.createElement("div");
  legend.className = `map-legend map-legend-${mapping.type}`;
  const bar = document.createElement("div");
  bar.className = "map-legend-bar";

  if (mapping.type === "categorical") {
    const categories = document.createElement("div");
    categories.className = "map-legend-categories";

    mapping.colors.forEach((color, index) => {
      const category = document.createElement("div");
      category.className = "map-legend-category";
      const swatch = document.createElement("span");
      swatch.className = "map-legend-swatch";
      swatch.style.backgroundColor = color;
      const label = document.createElement("span");
      label.className = "map-legend-category-label";
      label.style.setProperty("--category-color", color);
      const labelText = document.createElement("span");
      labelText.className = "map-legend-category-text";
      labelText.textContent = mapping.categories[index];
      label.append(labelText);
      category.append(swatch, label);
      categories.append(category);
    });
    legend.append(categories);
    return legend;
  }

  mapping.colors.forEach((color, index) => {
    const swatch = document.createElement("span");
    swatch.className = "map-legend-swatch";
    swatch.style.backgroundColor = color;
    bar.append(swatch);
  });
  const labels = document.createElement("div");
  labels.className = "map-legend-labels";
  labels.append("-", bar, "+");
  legend.append(labels);
  return legend;
}

function updateMapMapping(mapState, mapping) {
  mapState.mapping = mapping;
  hideMapPopup(mapState);
  if (mapState.map) {
    const layer = createMapDataLayer(mapState.data, mapping);
    for (const [property, value] of Object.entries(layer.paint)) {
      mapState.map.setPaintProperty("data", property, value);
    }
    const borderLayer = createMapBorderLayer(mapState.data, mapping);
    if (borderLayer) {
      for (const [property, value] of Object.entries(borderLayer.paint)) {
        mapState.map.setPaintProperty("data-border", property, value);
      }
    }
  }

  mapState.legend?.remove();
  mapState.legend = mapping ? createMapLegend(mapping) : null;
  if (mapState.legend) mapState.title.append(mapState.legend);
}

function hideMapPopup(mapState) {
  if (!mapState.popup) return;
  mapState.popupFeature = null;
  mapState.popupAnchor = null;
  mapState.popup.classList.remove("is-visible");
}

function sourceMapFeature(mapState, feature) {
  if (mapState.data?.type !== "FeatureCollection") return feature;
  const explicitFeature = mapState.data.features.find((item) => item.id === feature.id);
  const generatedFeature = Number.isInteger(feature.id)
    ? mapState.data.features[feature.id]
    : null;
  return explicitFeature || generatedFeature || feature;
}

function featureAnchor(feature) {
  const bounds = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
  const collect = (value) => {
    if (!Array.isArray(value)) return;
    if (value.length >= 2 && value.every((entry) => typeof entry === "number")) {
      bounds.minX = Math.min(bounds.minX, value[0]);
      bounds.minY = Math.min(bounds.minY, value[1]);
      bounds.maxX = Math.max(bounds.maxX, value[0]);
      bounds.maxY = Math.max(bounds.maxY, value[1]);
      return;
    }
    value.forEach(collect);
  };
  const collectGeometry = (geometry) => {
    if (!geometry) return;
    if (geometry.type === "GeometryCollection") geometry.geometries.forEach(collectGeometry);
    else collect(geometry.coordinates);
  };
  collectGeometry(feature.geometry);
  if (!Number.isFinite(bounds.minX)) return null;
  return [(bounds.minX + bounds.maxX) / 2, (bounds.minY + bounds.maxY) / 2];
}

function positionMapPopup(mapState) {
  if (!mapState.popupAnchor || !mapState.popup.classList.contains("is-visible")) return;
  const point = mapState.map.project(mapState.popupAnchor);
  mapState.popup.style.left = `${point.x}px`;
  mapState.popup.style.top = `${point.y}px`;
  requestAnimationFrame(() => {
    if (!mapState.popupAnchor) return;
    const halfWidth = mapState.popup.offsetWidth / 2;
    const left = Math.max(
      halfWidth + 4,
      Math.min(mapState.view.clientWidth - halfWidth - 4, point.x),
    );
    const pointerLeft = Math.max(
      7,
      Math.min(mapState.popup.offsetWidth - 7, point.x - left + halfWidth),
    );
    mapState.popup.style.left = `${left}px`;
    mapState.popup.style.setProperty("--popup-pointer-left", `${pointerLeft}px`);
  });
}

function showMapPopup(mapState, feature) {
  if (!mapState.mapping || !mapState.popup) return;
  const rawValue = feature.properties?.[mapState.mapping.key];
  mapState.popup.textContent = rawValue === null || rawValue === undefined || rawValue === ""
    ? "—"
    : String(rawValue);
  mapState.popupFeature = sourceMapFeature(mapState, feature);
  mapState.popupAnchor = featureAnchor(mapState.popupFeature);
  if (!mapState.popupAnchor) return;
  mapState.popup.classList.add("is-visible");
  positionMapPopup(mapState);
}

function toggleMapAttribute(mapState, key) {
  const mapping = mapState.mapping?.key === key ? null : mapState.attributeOptions.get(key);
  updateMapMapping(mapState, mapping);
  const activeButtons = mapState.attributeControls?.querySelectorAll(".map-attribute") || [];
  for (const button of activeButtons) {
    const active = button.dataset.attribute === mapping?.key;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-pressed", String(active));
  }
}

function createMapAttributeControls(mapState) {
  if (!mapState.attributeOptions.size) return null;
  const controls = document.createElement("div");
  controls.className = "map-attribute-controls";
  controls.setAttribute("role", "group");
  controls.setAttribute("aria-label", "Atributos temáticos");
  for (const key of mapState.attributeOptions.keys()) {
    const button = document.createElement("button");
    button.className = "map-attribute";
    button.type = "button";
    button.textContent = key;
    button.dataset.attribute = key;
    button.setAttribute("aria-pressed", "false");
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      toggleMapAttribute(mapState, key);
    });
    controls.append(button);
  }
  return controls;
}

async function closeMapView(mapState) {
  if (!mapState || mapState.closing) return;
  mapState.closing = true;
  mapState.closed = true;
  state.openMaps.delete(mapState);

  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  mapState.close.classList.remove("is-visible");
  if (!reducedMotion) {
    await new Promise((resolve) => window.setTimeout(resolve, 160));
  }

  const fromRect = mapState.card.getBoundingClientRect();
  mapState.resizeAnimation?.cancel();
  mapState.slot?.classList.remove("is-map-expanded");

  const finish = () => {
    hideFeatureSheet(mapState);
    mapState.map?.remove();
    mapState.titleParent.insertBefore(mapState.titleText, mapState.titleNextSibling);
    mapState.view.remove();
    mapState.card.classList.remove("is-map-active", "is-expanded", "is-resizing");
    mapState.card.setAttribute("aria-expanded", "false");
    if (state.expandedCard === mapState.card) state.expandedCard = null;
    fadeInResultContent(mapState.card);
  };

  const mapFade = mapState.view.animate(
    [{ opacity: 1 }, { opacity: 0 }],
    {
      duration: reducedMotion ? 0 : mapState.expands ? 420 : 180,
      easing: "ease-out",
      fill: "forwards",
    },
  );

  if (!mapState.expands) {
    mapFade.finished.catch(() => {}).then(finish);
    return;
  }

  mapState.resizeAnimation = animateResultResize(mapState.card, fromRect);
  Promise.all([
    mapState.resizeAnimation.finished.catch(() => {}),
    mapFade.finished.catch(() => {}),
  ]).then(finish);
}

function fadeInResultContent(card) {
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const content = card.querySelector(":scope > .preview");
  content?.animate(
    [{ opacity: 0 }, { opacity: 1 }],
    { duration: 180, easing: "ease-out" },
  );
}

function animateResultResize(card, fromRect) {
  clearResultResizeStyles(card);
  const toRect = card.getBoundingClientRect();
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (reducedMotion || !fromRect.width || !toRect.width) {
    return card.animate([], { duration: 0 });
  }

  card.classList.add("is-resizing");
  Object.assign(card.style, {
    position: "fixed",
    inset: "auto",
    top: `${fromRect.top}px`,
    left: `${fromRect.left}px`,
    width: `${fromRect.width}px`,
    height: `${fromRect.height}px`,
  });
  const animation = card.animate(
    [
      {
        top: `${fromRect.top}px`,
        left: `${fromRect.left}px`,
        width: `${fromRect.width}px`,
        height: `${fromRect.height}px`,
      },
      {
        top: `${toRect.top}px`,
        left: `${toRect.left}px`,
        width: `${toRect.width}px`,
        height: `${toRect.height}px`,
      },
    ],
    { duration: 420, easing: "cubic-bezier(.22,.72,.24,1)" },
  );
  resultResizeAnimations.set(card, animation);
  animation.finished.catch(() => {}).then(() => {
    if (resultResizeAnimations.get(card) !== animation) return;
    resultResizeAnimations.delete(card);
    clearResultResizeStyles(card);
    card.classList.remove("is-resizing");
  });
  return animation;
}

function clearResultResizeStyles(card) {
  for (const property of ["position", "inset", "top", "left", "width", "height"]) {
    card.style.removeProperty(property);
  }
}

function hideFeatureSheet(mapState) {
  if (!mapState.sheet) return;
  mapState.sheet.remove();
  mapState.sheet = null;
}

function showFeatureSheet(mapState, properties) {
  const attributes = createAttributeSection(
    Object.entries(properties || {}),
    "",
    false,
  );
  if (!attributes) {
    hideFeatureSheet(mapState);
    return;
  }

  const currentTable = mapState.sheet?.querySelector(".attribute-table");
  const nextTable = attributes.querySelector(".attribute-table");
  if (currentTable && nextTable) {
    const scrollTop = currentTable.scrollTop;
    currentTable.replaceChildren(...nextTable.children);
    requestAnimationFrame(() => {
      currentTable.scrollTop = scrollTop;
    });
    return;
  }

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

  sheet.append(handle);
  sheet.append(attributes);
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
  const setHoveredFeature = (feature) => {
    const nextId = feature?.id ?? null;
    if (mapState.hoveredFeatureId === nextId) return;
    if (mapState.hoveredFeatureId !== null) {
      mapState.map.setFeatureState(
        { source: "data", id: mapState.hoveredFeatureId },
        { hovered: false },
      );
    }
    mapState.hoveredFeatureId = nextId;
    if (nextId !== null) {
      mapState.map.setFeatureState({ source: "data", id: nextId }, { hovered: true });
    }
  };
  const featureAtPoint = (event) => mapState.map.queryRenderedFeatures(
    [
      [event.point.x - 10, event.point.y - 10],
      [event.point.x + 10, event.point.y + 10],
    ],
    { layers: ["data"] },
  )[0];
  const selectFeature = (event) => {
    const feature = featureAtPoint(event);
    if (!feature) return;
    if (mapState.selectedFeatureId === feature.id && !usesDesktopHover()) {
      if (mapState.popup.classList.contains("is-visible")) hideMapPopup(mapState);
      else showMapPopup(mapState, feature);
      return;
    }
    if (mapState.selectedFeatureId !== null) {
      mapState.map.setFeatureState(
        { source: "data", id: mapState.selectedFeatureId },
        { selected: false },
      );
    }
    mapState.selectedFeatureId = feature.id;
    mapState.map.setFeatureState({ source: "data", id: feature.id }, { selected: true });
    showFeatureSheet(mapState, feature.properties);
    showMapPopup(mapState, feature);
  };
  mapState.map.on("click", selectFeature);
  mapState.map.on("move", () => positionMapPopup(mapState));
  mapState.map.on("mousemove", (event) => {
    const feature = featureAtPoint(event);
    setHoveredFeature(usesDesktopHover() ? feature : null);
    if (!usesDesktopHover()) return;
    if (feature) showMapPopup(mapState, feature);
    else hideMapPopup(mapState);
  });
  mapState.map.getCanvas().addEventListener("mouseleave", () => {
    setHoveredFeature(null);
    hideMapPopup(mapState);
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
  const slot = card.parentElement;
  const columnCount = getComputedStyle(els.results).gridTemplateColumns.trim().split(/\s+/).length;
  const expands = slot && columnCount >= 2;
  const fromRect = card.getBoundingClientRect();
  card.classList.add("is-map-active");
  if (expands) slot.classList.add("is-map-expanded");
  if (state.expandedCard === card) state.expandedCard = null;

  const canvas = document.createElement("div");
  canvas.className = "map-canvas";
  canvas.setAttribute("aria-label", `Mapa de ${item.titulo}`);

  const title = document.createElement("div");
  title.className = "map-title";
  const titleText = card.querySelector(".details header > .title");
  const titleParent = titleText.parentElement;
  const titleNextSibling = titleText.nextSibling;
  title.append(titleText);

  const popup = document.createElement("div");
  popup.className = "map-feature-popup";
  popup.setAttribute("aria-live", "polite");

  const mapState = {
    card,
    slot,
    expands,
    view,
    map: null,
    sheet: null,
    selectedFeatureId: null,
    hoveredFeatureId: null,
    sheetHeight: null,
    hasShownSheet: false,
    data: null,
    attributeOptions: new Map(),
    mapping: null,
    legend: null,
    title,
    titleText,
    titleParent,
    titleNextSibling,
    popup,
    popupFeature: null,
    popupAnchor: null,
    attributeControls: null,
    close: null,
    closed: false,
    closing: false,
    resizeAnimation: null,
  };
  const close = document.createElement("button");
  close.className = "map-close";
  close.type = "button";
  close.setAttribute("aria-label", "Cerrar mapa");
  close.textContent = "×";
  close.addEventListener("click", () => closeMapView(mapState));
  mapState.close = close;

  const message = document.createElement("div");
  message.className = "map-message";
  message.setAttribute("aria-live", "polite");
  message.append(createLoadingDots());

  view.append(canvas, title, popup, close, message);
  view.addEventListener("click", (event) => event.stopPropagation());
  card.append(view);
  state.openMaps.add(mapState);
  if (expands) {
    mapState.resizeAnimation = animateResultResize(card, fromRect);
    mapState.resizeAnimation.finished.catch(() => {}).then(() => {
      if (!mapState.closing) close.classList.add("is-visible");
    });
  } else {
    requestAnimationFrame(() => {
      if (!mapState.closing) close.classList.add("is-visible");
    });
  }
  return { mapState, canvas, message };
}

async function openArchiveMap(item, card) {
  const { mapState, canvas, message } = createMapView(item, card);

  try {
    const response = await fetch(archiveGeojsonProxyUrl(item.archiveItem, item.geojsonUrl));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (mapState.closed) return;
    const mapData = prepareMapGeojson(data, item.epsg);
    mapState.data = mapData;
    mapState.attributeOptions = analyzeMapAttributes(mapData);

    const dark = darkModeQuery.matches;
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
          data: { type: "geojson", data: mapData, generateId: true },
          "carto-labels": {
            type: "raster",
            tiles: labelTiles,
            tileSize: 256,
          },
        },
        layers: [
          { id: "carto-light", type: "raster", source: "carto-light" },
          { id: "data", source: "data", ...createMapDataLayer(mapData) },
          createMapBorderLayer(mapData),
          { id: "carto-labels", type: "raster", source: "carto-labels" },
        ].filter(Boolean),
      },
      attributionControl: false,
    });
    mapState.map = map;
    mapState.attributeControls = createMapAttributeControls(mapState);
    if (mapState.attributeControls) mapState.title.append(mapState.attributeControls);
    bindFeatureInteraction(mapState);
    map.on("load", () => {
      message.remove();
      map.fitBounds(geojsonBounds(mapData), { padding: 28, maxZoom: 14, duration: 0 });
    });
  } catch (error) {
    message.replaceChildren(createStatusIcon("error"));
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

function formatDownloadSize(bytes) {
  if (!bytes) return null;
  return `${(bytes / 1_000_000).toFixed(2)} MB`;
}

function createDownloadChip(label, href, bytes) {
  const chip = createChip(label, href);
  const size = formatDownloadSize(bytes);
  if (chip && size) {
    chip.classList.add("has-tooltip");
    chip.dataset.tooltip = size;
  }
  return chip;
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

function createAttributeSection(
  sample,
  title = "Muestra de Atributos",
  showTitle = true,
) {
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
  node.addEventListener("mouseenter", () => {
    if (usesDesktopHover()) openCard(node);
  });
  node.addEventListener("mouseleave", () => {
    if (!usesDesktopHover()) return;
    if (node.matches(":focus-within")) return;
    if (state.expandedCard === node) {
      closeExpandedCard();
    }
  });
  node.addEventListener("focusin", () => {
    if (usesDesktopHover()) openCard(node);
  });
  node.addEventListener("focusout", () => {
    if (!usesDesktopHover()) return;
    queueMicrotask(() => {
      if (!node.matches(":focus-within") && state.expandedCard === node) {
        closeExpandedCard();
      }
    });
  });
  node.addEventListener("click", (event) => {
    const clickedControl = event.target.closest("button, a, input, select, textarea");
    if (clickedControl) return;
    if (window.getSelection()?.toString()) return;
    if (usesDesktopHover()) return;
    if (state.expandedCard === node) closeExpandedCard();
    else openCard(node);
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
      createDownloadChip("GeoParquet", item.geoparquetUrl, item.geoparquetBytes),
      createDownloadChip("GeoJSON", item.geojsonUrl, item.geojsonBytes),
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

function createResultSlot(card) {
  const slot = document.createElement("div");
  slot.className = "result-slot";
  slot.append(card);
  return slot;
}

function renderDataset(mode) {
  const items = state.datasets[mode] || [];
  const filtered = filterItems(items);
  const visible = filtered.slice(0, state.limit);
  const renderer = mode === "archivo" ? renderArchiveCard : renderDiscoverCard;

  closeAllMaps();
  closeExpandedCard();
  els.emptyState.classList.remove("status-state");
  els.results.replaceChildren(...visible.map(renderer).map(createResultSlot));
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
  renderLoading();
}

function renderLoading() {
  closeExpandedCard();
  els.results.replaceChildren();
  els.results.classList.add("is-hidden");
  els.emptyState.classList.add("is-hidden");
  els.emptyState.classList.remove("status-state");
  els.placeholderState.classList.remove("is-hidden");
  const loadingHeading = els.placeholderState.querySelector("h2");
  loadingHeading.replaceChildren(createLoadingDots());
  loadingHeading.setAttribute("aria-live", "polite");
  loadingHeading.setAttribute("aria-label", "Cargando");
  els.placeholderState.querySelector("p").classList.add("is-hidden");
  els.resultsSummary.textContent = "";
  els.footer.classList.add("is-hidden");
  els.loadMore.classList.add("is-hidden");
}

function createLoadingDots() {
  const dots = document.createElement("span");
  dots.className = "loading-dots";
  dots.setAttribute("aria-hidden", "true");
  for (let index = 0; index < 3; index += 1) {
    const dot = document.createElement("span");
    dot.className = "loading-dot";
    dots.append(dot);
  }
  return dots;
}

function createStatusIcon(kind) {
  const icon = document.createElement("span");
  icon.className = `status-icon status-${kind}`;
  icon.setAttribute("role", "img");
  icon.setAttribute("aria-label", kind === "error" ? "No se pudo cargar" : "Cargando");
  icon.textContent = kind === "error" ? "!" : "";
  return icon;
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
      els.emptyState.classList.add("status-state");
      const errorMessage = els.emptyState.querySelector("p");
      errorMessage.replaceChildren(createStatusIcon("error"));
      errorMessage.setAttribute("aria-live", "polite");
      els.resultsSummary.textContent = "";
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
