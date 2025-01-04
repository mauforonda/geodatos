import {mostrarCapa} from "./mostrar.f7807c8f.js"
import {csv, json} from "../../_npm/d3@7.9.0/_esm.js"

export async function cargarCapas() {
    const baseUrl = "https://raw.githubusercontent.com/mauforonda/geodatos/master"
    async function cargarDirectorio() {
        const d = await json(`${baseUrl}/directorio.json`)
        return Object.fromEntries(d.map((i) => [i.nombre, i]))
    }
    const directorio = await cargarDirectorio()
    const c = await csv(`${baseUrl}/capas.csv`)
    return c.filter(i => !i.fecha_removido).map(i => { return {...i, presentacion:mostrarCapa(i, directorio)} }).reverse()
}