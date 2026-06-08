# Informe - Laboratorio 3: Procesamiento Distribuido con Apache Spark

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo y tipos en Scala

El pipeline del programa sigue la siguiente secuencia de pasos:

FLUJO DEL PIPELINE REDDIT NER

DRIVER —> Lectura del archivo inicial

**--- PASO 1: Lectura de suscripciones ---**

- **Input:** subscriptions.json (archivo)

- **Output:** List[Subscription]

- **Tipo:** List[Subscription]

**--- PASO 2: Descarga de feeds ⟿ PARALELIZABLE ---**

- **Input:** List[Subscription]

- **Output:** List[(Option[String], Subscription)]
  (JSON del feed o None si falla)

- **Tipo:** List[(Option[String], Subscription)]

Independiente por cada suscripción

**--- PASO 3: Parseo JSON de posts ⟿ PARALELIZABLE ---**

• **Input:** List[(Option[String], Subscription)]

• **Output:** List[Post]

• **Tipo:** List[Post]

Cada feed se procesa en paralelo

**--- PASO 4: Filtrado de posts vacíos ⟿ PARALELIZABLE ---**

• **Input:** List[Post]

• **Output:** List[Post] (solo válidos)

• **Tipo:** List[Post]

Evaluación independiente por post

**--- PASO 5: Extracción de entidades ⟿ PARALELIZABLE ---**

• **Input:** List[Post]

• **Output:** List[NamedEntity]

• **Tipo:** List[NamedEntity]

Procesamiento paralelo de posts

**--- PASO 6: Conversión a pares (tipo, nombre) → 1 ⟿ PARALELIZABLE ---**

• **Input:** List[NamedEntity]

• **Output:** List[((String, String), Int)]

• **Tipo:** List[((String, String), Int)]

Transformación independiente por entidad

**--- PASO 7: Conteo de entidades - reduceByKey ⟿ BARRERA ---**

• **Input:** List[((String, String), Int)]

• **Output:** List[((String, String), Int)] (con totales)

• **Tipo:** List[((String, String), Int)]

SINCRONIZACIÓN: todos los workers deben terminar

**--- PASO 8: Ranking y formateo → DRIVER ---**

• **Input:** List[((String, String), Int)]

• **Output:** Resultados ordenados por pantalla

• **Tipo:** String

Ordenamiento: descendente por conteo, alfabético por tipo

### Tabla resumen de tipos Scala por conexión:

```
 De → A            Paso                Tipo Input                              Tipo Output
 ----------------  ------------------  --------------------------------------  --------------------------------------
 DRIVER → Lectura  1                   `String` (archivo)                      `List[Subscription]`
 1 → 2             Descarga feeds      `List[Subscription]`                    `List[(Option[String], Subscription)]`
 2 → 3             Parseo              `List[(Option[String], Subscription)]`  `List[Post]`
 3 → 4             Filtrado            `List[Post]`                            `List[Post]`
 4 → 5             Extracción NER      `List[Post]`                            `List[NamedEntity]`
 5 → 6             Conversión a pares  `List[NamedEntity]`                     `List[((String, String), Int)]`
 6 → 7             Conteo (reduce)     `List[((String, String), Int)]`         `List[((String, String), Int)]`
 7 → 8             Ranking             `List[((String, String), Int)]`         `String` (salida)
```
### d) El mecanismo de extensión de Spark: restricciones sobre las funciones pasadas a transformaciones

### 1. Serialización

Para que una función pueda viajar del driver (el programa principal, el que coordina todo) a los workers por red, tiene que ser convertible a bytes. Esto se llama **serialización**, y en la JVM requiere que el objeto implemente `java.io.Serializable`.

El problema práctico surge con los **closures** (una función que "recuerda" variables del contexto donde fue definida — si definís una función dentro de un método y esa función usa variables de ese método, esas variables quedan "capturadas" dentro de la función). Cuando una función anónima en Scala referencia una variable externa, el compilador captura esa variable en el closure. Si la variable pertenece a una instancia de una clase, Spark necesita serializar **toda** esa instancia, no solo el valor puntual. Si la clase contiene algún campo no serializable (una conexión a base de datos, un stream abierto, etc.), Spark lanza un error en tiempo de ejecución.

Hay dos formas de escribir funciones que evitan este problema:

- Las **funciones anónimas** que no referencian estado externo no tienen nada extra que serializar — el closure contiene solo la función misma.
- Los **`object` singleton de Scala** son equivalentes a clases con miembros estáticos en Java: no existe una instancia, solo una referencia al símbolo. Spark no necesita serializar ningún objeto, lo cual garantiza que la serialización siempre funcione independientemente de qué otros campos pueda tener la clase.

> La distinción importante es que no se trata de una preferencia estética: estas dos formas son las **únicas** que garantizan serialización exitosa en todos los casos. Con métodos de una `class` común, la serialización puede funcionar o fallar dependiendo del contenido de la instancia, lo cual hace el comportamiento impredecible.

---

### 2. Estado compartido

Esta es la restricción más importante desde el punto de vista de los paradigmas de programación. Spark asume que las funciones pasadas a transformaciones **no modifican estado compartido mutable externo** (variables globales o de instancia que múltiples partes del programa pueden leer y escribir al mismo tiempo).

La razón es el modelo de ejecución: Spark puede ejecutar la misma tarea más de una vez. Si un worker falla a mitad de un job, Spark re-ejecuta las tareas de esa partición en otro nodo. Si una función tiene efectos sobre estado mutable compartido, esa re-ejecución produce resultados incorrectos o duplicados.

Para los dos casos legítimos de estado compartido, Spark expone mecanismos explícitos con semánticas que puede garantizar en un entorno distribuido:

#### Broadcast variables — para que todos los workers lean un dato grande sin duplicarlo

Cuando una función necesita acceder a un dato grande (una tabla de referencia, un diccionario de miles de entradas), la forma directa es capturarlo en el closure. El problema es que Spark manda una copia de ese dato junto con cada tarea. Si hay 10.000 tareas y el dato ocupa 50MB, se están transfiriendo 500GB de datos por red solo para repartir ese dato.

Una broadcast variable resuelve esto: Spark distribuye **una sola copia por worker**. Todas las tareas que corren en ese worker comparten esa copia en memoria local, sin tráfico de red adicional. La semántica es estrictamente de **solo lectura**: el valor no puede modificarse una vez distribuido, y eso es precisamente lo que permite a Spark garantizar que todos los workers ven exactamente el mismo dato.

```scala
val tabla = Map("AR" -> "Argentina", "BR" -> "Brasil", "CL" -> "Chile")
val tablaBcast = sc.broadcast(tabla)

val rdd = sc.parallelize(Array("AR", "BR", "CL", "AR"))
val nombres = rdd.map(codigo => tablaBcast.value.getOrElse(codigo, "Desconocido"))
// Cada worker tiene una sola copia de `tabla` en memoria local.
// Las miles de tareas que corren en ese worker la leen desde ahí,
// sin tráfico de red extra por cada tarea.
```

#### Accumulators — para que los workers reporten valores al driver

Para el caso inverso — que los workers necesiten reportar valores al driver (conteos, sumas, métricas) — Spark provee los **accumulators**. Son variables que solo soportan la operación `add` (sumar o agregar un valor), que debe ser asociativa y conmutativa (es decir, el orden en que se combinen los resultados parciales no importa). Esto permite que Spark combine los aportes de cada worker sin que tengan que coordinarse entre sí, y sin condiciones de carrera (situaciones donde dos procesos intentan modificar el mismo dato al mismo tiempo con resultados impredecibles).

La asimetría es deliberada: los workers solo pueden **escribir** (`add`), únicamente el driver puede **leer** el valor final. Esta restricción es la que hace posible la garantía de correctitud en un entorno paralelo.

```scala
val erroresAcc = sc.longAccumulator("Errores")

val logsRelevantes = logs.filter { linea =>
  if (linea.contains("ERROR")) erroresAcc.add(1)
  linea.contains("ERROR") || linea.contains("WARNING")
}

logsRelevantes.count()  // acción que dispara la ejecución real

println(s"Errores encontrados: ${erroresAcc.value}")
// Solo el driver puede leer este valor
```

---

### 3. Efectos secundarios

Un **efecto secundario** es cualquier cosa que una función hace además de calcular y devolver su resultado: escribir a un archivo, llamar a una API, modificar una variable externa, imprimir por consola. Spark no tiene ningún mecanismo que detecte o impida efectos secundarios en las funciones que recibe — el código va a compilar y ejecutarse sin errores visibles. El problema es que el modelo de ejecución de Spark hace que esos efectos produzcan resultados incorrectos o impredecibles sin advertencia alguna.

Hay tres razones concretas:

1. La **evaluación lazy** hace que los efectos secundarios ocurran en un momento no predecible: las transformaciones no se ejecutan cuando se escriben sino cuando una acción las fuerza, así que si una función escribe a un log dentro de un `map`, esa escritura no ocurre donde uno esperaría en el código.
2. La **posible re-ejecución de tareas** hace que si un worker falla, Spark re-ejecute esa tarea en otro nodo — un efecto secundario como escribir a una base de datos puede ocurrir dos veces para el mismo dato sin que nada lo indique.
3. El **orden de ejecución no determinístico** entre particiones implica que si hay dependencias entre efectos de distintas tareas, no hay garantías sobre el orden en que van a ocurrir.

Por todo esto, Spark impone implícitamente — sin verificación estática — que las funciones sean **referencialmente transparentes** (que dado el mismo input siempre produzcan el mismo output sin hacer nada más). Las únicas excepciones controladas son los mecanismos que el framework provee explícitamente para esto: accumulators para reportar valores al driver, y acciones como `saveAsTextFile` para efectos sobre el sistema de archivos con semántica definida.

---

### Síntesis

Las tres restricciones no son arbitrarias: son consecuencias directas del modelo de ejecución distribuido.

| Restricción | Causa raíz |
|---|---|
| **Serialización** | El código tiene que viajar por red desde el driver hasta cada worker |
| **Estado mutable compartido** | Spark puede re-ejecutar tareas y múltiples workers operan en paralelo sin coordinación |
| **Efectos secundarios** | La evaluación lazy y el no-determinismo en el orden de ejecución |

Broadcast variables y accumulators son la superficie controlada que Spark expone para los únicos dos patrones de estado compartido que puede garantizar correctamente: **lectura eficiente de datos grandes** y **acumulación de resultados parciales**.