export const INDEX_TYPES = {
  descubrir: {
    id: "descubrir",
    nombre: "Descubiertos",
    descripcionCorta: "Datos disponibles en sus fuentes originales",
    busquedaPlaceholder: "Buscar por nombre, descripción o fuente ...",
  },
  archivo: {
    id: "archivo",
    nombre: "Archivados",
    descripcionCorta: "Datos archivados independientemente",
    busquedaPlaceholder: "Buscar por nombre, descripción, fuente o atributos ...",
  },
};

export function getIndexType(mode) {
  return INDEX_TYPES[mode] || INDEX_TYPES.descubrir;
}
