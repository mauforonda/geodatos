---
theme: [air, ink, wide]
---

<link rel="stylesheet" href="style.css">

```js
import {cargarCapas} from "./components/cargar.js";
const capas = await cargarCapas()
```

```js
const busqueda = Inputs.search(capas, {
  placeholder: 'Busca ...',
  columns: ["nombre", "geoserver", "titulo", "descripcion"],
  format: d => `${d} capas`
})
const busquedaValor = Generators.input(busqueda)
```

```js
const tabla = Inputs.table(busquedaValor, {
  columns: ["presentacion"],
  format: {
    presentacion: (d) => d
  },
  header: {
    presentacion: ""
  },
  rows: 35
})
```
<div class="">
  <div class="listado">
    <div class="busquedaContenedor">
      ${busqueda}
    </div>
    <div class="tablaContenedor">
    ${tabla}
    </div>
  </div>
</div>

<div class="footer">Una lista actualizada de datos geográficos sobre Bolivia creada por <a href="mailto:mauriforonda@gmail.com">Mauricio Foronda</a></div>