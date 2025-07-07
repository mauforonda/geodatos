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
  rows: 25,
  height: 900
})
```
<div class="header">
  <div class="title">
    GeoDatos sobre Bolivia
  </div>
  <div class="subtitle">
    Un directorio de datos geográficos en servicios del gobierno boliviano
  </div>
</div>

<div class="">
  <div class="listado">
    <div class="busquedaContenedor">${busqueda}</div>
    <div class="tablaContenedor">${tabla}</div>
  </div>
</div>

<div class="footer">
  <div class="plant">🪴</div>
  <div class="footerEntry">
    Actualizado a diario en <a href="https://github.com/mauforonda/geodatos/">Github</a>
  </div>
  <div class="footerEntry">
    Creado por <a href="mailto:mauriforonda@gmail.com">Mauricio Foronda</a>
  </div>
</div>