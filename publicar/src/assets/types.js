export const INDEX_TYPES = {
  descubrir: {
    id: "descubrir",
    nombre: "Descubiertos",
    descripcionCorta: "Datos geográficos disponibles en sus fuentes originales",
    descripcionLarga:
      "Datos vectoriales o raster disponibles hasta anoche.",
    busquedaPlaceholder: "Por nombre, descripción o fuente ...",
  },
  archivo: {
    id: "archivo",
    nombre: "Archivados",
    descripcionCorta: "Datos geográficos archivados de manera independiente",
    descripcionLarga:
      "Datos vectoriales resguardados diariamente en Internet Archive.",
    busquedaPlaceholder: "Por nombre, descripción, fuente o atributos ...",
  },
};

export function getIndexType(mode) {
  return INDEX_TYPES[mode] || INDEX_TYPES.descubrir;
}
