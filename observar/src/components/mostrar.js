import { html } from "npm:htl";

export function mostrarCapa(capa, directorio) {
    function mostrarNombre(capa) {
        return html`<div capa="nombre">
            <div class="titulo" title="${capa.nombre}">${capa.titulo}</div>
        </div>`;
    }

    function mostrarDescripcion(descripion) {
        if (descripion) {
            return html`<div class="descripcion">${capa.descripcion}</div>`;
        } else {
            return ``;
        }
    }

    function mostrarGeoserver(geoserver) {
        return html`<div class="geoserver">
            <span>en </span>${directorio[geoserver].descripcion}
        </div>`;
    }

    function mostrarFechas(capa) {
        function mostrarFecha(nombre, fecha) {
            const dateFormatter = new Intl.DateTimeFormat("es", {
                year: "numeric",
                month: "long",
                day: "numeric",
                timeZone: "UTC",
            });
            const f = new Date(fecha)
            if (fecha) {
                return html`<div class="fecha">${nombre} el ${dateFormatter.format(f)}</div>`;
            } else {
                return ``;
            }
        }

        return html`<div class="fechas">
            ${mostrarFecha("encontrado", capa.fecha_encontrado)}
            ${mostrarFecha("removido", capa.fecha_removido)}
        </div>`;
    }

    function link(url, texto, clase = "") {
        return html`<a
            class="enlace ${clase}"
            target="_blank"
            href="${url}"
            style=""
            >${texto}</a
        >`;
    }

    function wmsUrl(capa, formato, width = 500, height = 500) {
        const ows = directorio[capa.geoserver].ows;
        if (ows.match(/\/ows$/g)) {
            return `${ows.replace("//ows$/i", "")}/wms/reflect?layers=${
                capa.nombre
            }&format=${formato}`;
        } else {
            return `${ows}?service=wms&request=GetMap&layers=${capa.nombre}&format=${formato}&width=${width}&height=${height}&bbox=${capa.min_x},${capa.min_y},${capa.max_x},${capa.max_y}`;
        }
    }

    function linkWms(capa, formato, texto, clase = "enlaceWms") {
        return link(wmsUrl(capa, formato), texto, clase);
    }

    function linkWfs(capa, formato, texto, clase = "enlaceWfs") {
        return link(
            `${
                directorio[capa.geoserver].ows
            }?service=wfs&request=GetFeature&typeName=${
                capa.nombre
            }&outputFormat=${formato}`,
            texto,
            clase
        );
    }

    function imagen(capa, width = 50, height = 50) {
        const ows = directorio[capa.geoserver].ows;
        const formato = "image/jpeg";
        let url;
        if (ows.match(/\/ows$/g)) {
            url = `${ows.replace("//ows$/i", "")}/wms/reflect?layers=${
                capa.nombre
            }&format=${formato}&transparent=true&width=${width}&height=${height}`;
        } else {
            url = `${ows}?service=wms&request=GetMap&layers=${capa.nombre}&format=${formato}&transparent=true&width=${width}&height=${height}&bbox=${capa.min_x},${capa.min_y},${capa.max_x},${capa.max_y}`;
        }

        return html`<div
            class="imagen"
            style="background-image:url(${url});"
        ></div>`;
    }

    return html`<div class="capa">
        <div class="imagenContenedor">${imagen(capa, 80, 80)}</div>
        <div>
            <div class="nombreContenedor">
                ${mostrarNombre(capa)} ${mostrarDescripcion(capa.descripcion)}
                ${mostrarGeoserver(capa.geoserver)}
            </div>
            ${mostrarFechas(capa)}
            <div class="enlaces">
                ${linkWms(capa, "application/openlayers", "ver mapa", "")}
                ${linkWfs(capa, "application/json&count=1", "ver ejemplo", "")}
                ${linkWfs(capa, "application/json", "GeoJSON")}
                ${linkWfs(capa, "SHAPE-ZIP", "ShapeFile")}
                ${linkWfs(capa, "csv", "CSV")}
                ${linkWms(capa, "image/geotiff", "GeoTIFF")}
                ${linkWms(capa, "application/vnd.google-earth.kml", "KML")}
                ${linkWms(capa, "image/jpeg", "JPEG")}
                ${linkWms(capa, "application/pdf", "PDF")}
            </div>
        </div>
    </div>`;
}
