import {mostrarCapa} from "./mostrar.js"
import {csv, json} from "npm:d3"

export async function cargarCapas() {
    const baseUrl = "https://raw.githubusercontent.com/mauforonda/geodatos/master"
    async function cargarDirectorio() {
        const d = await json(`${baseUrl}/directorio.json`)
        return Object.fromEntries(d.map((i) => [i.nombre, i]))
    }
    const directorio = await cargarDirectorio()
    const c = await csv(`${baseUrl}/capas.csv`)
    return c.map(i => { return {...i, presentacion:mostrarCapa(i, directorio)} }).reverse()
}