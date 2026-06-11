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

**--- PASO 6: Conversión a pares (tipo, nombre) → valor ⟿ PARALELIZABLE ---**

• **Input:** List[NamedEntity]

• **Output:** List[((String, String), Int)]

• **Tipo:** List[((String, String), Int)]

Transformación independiente por entidad

**--- PASO 7: Conteo de entidades - reduceByKey ⟿ BARRERA ---**

• **Input:** List[((String, String), Int)]

• **Output:** Map[(String, String), Int]

• **Tipo:** Map[(String, String), Int]

Nota: **BARRERA DE SINCRONIZACIÓN**: Todos los workers deben terminar sus tareas locales antes de combinar los resultados por red.

**--- PASO 8: Ranking y formateo → DRIVER ---**

• **Input:** Map[(String, String), Int]

• **Output:** Resultados ordenados por pantalla

• **Tipo:** String

Ordenamiento: descendente por conteo, alfabético por tipo

### Tabla resumen de tipos Scala por conexión:

```
 De → A            Paso                Tipo Input                              Tipo Output
 ----------------  ------------------  --------------------------------------  ------------------------
 DRIVER → Lectura  1                   `String` (archivo)                      `List[Subscription]`
 1 → 2             Descarga feeds      `List[Subscription]`                    `List[(Option[String], Subscription)]`
 2 → 3             Parseo              `List[(Option[String], Subscription)]`  `List[Post]`
 3 → 4             Filtrado            `List[Post]`                            `List[Post]`
 4 → 5             Extracción NER      `List[Post]`                            `List[NamedEntity]`
 5 → 6             Conversión a pares  `List[NamedEntity]`                     `List[((String, String), Int)]`
 6 → 7             Conteo (reduce)     `List[((String, String), Int)]`         `Map[(String, String), Int]`
 7 → 8             Ranking             `Map[(String, String), Int]`            `String` (salida)
```

---

### b) Identificación de abstracciones de Spark

### Desarrollo y deducción

Entendemos por **PIPELINE** a la secuencia de pasos que transforma los datos de entrada
en el resultado final del programa:

**1 -> 2 -> 3 -> 4 .... -> Fin**

Donde:

**Output(1) -> Input(2) -> .... -> Output(Fin)**

Dentro de un Pipeline podremos tener dos tipos de **sucesos**:

### Acciones

Las acciones son procesos que toman una **RDD (Resilient Distributed Database)** y devuelven un resultado inmediato que escapa por fuera del flujo de un pipeline

**Ejemplos:**

- count
- collect
- foreach
- print

### Transformaciones

Son las diferentes etapas por las cuales se va pasando en el flujo del Pipeline, estos no tienen resultados inmediatos y solo son pasos iniciales, intermedios y finales para llegar de un **"Input General"** hasta el **"Output General"**.
Toman un RDD y devuelven otro RDD. **Son Lazy**, no se ejecutan hasta que alguien las necesita.

**Ejemplos:**

- map
- flatMap
- reduceByKey

## NUESTRO PIPELINE

NUESTRO PIPELINE (Análisis del Esqueleto)

```
  1. Lectura de suscripciones
  2. Descarga de feeds
  3. Parseo JSON de posts
  4. Filtrado de posts vacíos
  5. Extracción de entidades NER
  6. Conversión a pares ((tipo, nombre), 1)
  7. Conteo de entidades (reduceByKey)
  8. Ranking y formateo
```

**OBSERVACIÓN**: A diferencia del esqueleto, En el Pipeline pueden fusionarse en un único flatMap la Descarga de feeds y el aplanamiento de los datos, tanto en Scala secuencial como en Spark. En el skeleton están separados por claridad expositiva. En nuestra implementación con Spark los fusionamos para evitar RDDs intermedios innecesarios (ya que conllevan overhead, pero nos abstraeremos de ello)

### Lo que SI se puede representar como una Abstracción de Spark

- 2 -> **flatMap**: Por cada subscripción puede devolver **VARIOS o NINGÚN feed**

- 3 -> **flatMap**: Por cada feed puede devuelver **VARIOS o NINGÚN post**.

- 4 -> **"filter"**: Si bien no está explícito en la consigna, vale la pena mencionarlo ya que sacamos posts vacíos, si lo queremos ver a modo de **flatMap**, donde puede devolver 1 o 0 elementos (no usamos **map** pues debería consistir en que devuelva EXACTAMENTE un solo elemento).

- 5 -> **flatMap**: Por cada Post pueden surgir **VARIAS o NINGUNA entidad**.

- 6 -> **map**: Por cada entidad lo mapeamos a una **ÚNICA TUPLA (Clave,Valor)**.

- 7 -> **reduceByKey u otras reducciones**: Por cada entidad se organiza en base a una **Clave** y se colapsan los **Valores**.

### Lo que NO se puede representar como una Abstracción de Spark

- 1 -> **Es el Driver**: Leer subscripciones es "preparar el terreno" para poder trabajar con paralelismo con Spark. Descartamos las 3 opciones pues:
  - **map y flatMap**: Necesitamos como input un **RDD**, y como input tenemos un **String**.
  - **Reducciones**: necesita pares clave-valor distribuidos para combinar.

  Y una pista clave la encontramos en el Ejercicio 2 cuando nos dicen:

  _"Lean las suscripciones de subscriptions.json y cárguenlas en un RDD con
  sc.parallelize(subscriptions)"_.

- 8 -> **Es el Driver (Ranking e Impresiones en pantalla)**: Es la etapa final del programa. El ordenamiento descendente del ranking y el formateo de texto son procesos secuenciales que se ejecutan de manera central en el Driver. No pueden ser transformaciones de Spark porque no generan un nuevo RDD, sino que consumen el resultado final distribuido mediante **acciones (como collect)** para generar un **efecto secundario (la salida en consola)**.

### c) Barreras de sincronización y paralelismo

#### Partes completamentes independientes son:

- FileIO.downloadFeed(...)
- JsonParser.parsePosts(...)
- Analizer.filterEmptyPosts(...)
- Analyzer.detectEntities(...)

#### Partes que son barreras de sincronización (requiere que todos los workers terminen):

- downloadResults.count(...)
- if (filteredPosts.isEmpty) - verificación de post vacios
- Analizer.countEntities
- Analizer.countByType
- .sortBy{...}

Los pasos de descarga, parseo, filtrado y detección de entidades son completamente independientes, cada worker opera sobre su porción de datos sin coordinarse con otros, y el resultado de uno no afecta al de los demás.
Las barreras aparecen exactamente donde el resultado depende de la colección completa, cálculo de estadísticas globales (conteos, promedios), el chequeo de isEmpty, y las dos agregaciones por clave (countEntities y countByType). En estas etapas Spark ejecuta un shuffle, redistribuye los datos entre workers según la clave, y ningún worker puede producir su parte del resultado final hasta que todos hayan terminado la fase anterior.

---

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

| Restricción                   | Causa raíz                                                                             |
| ----------------------------- | -------------------------------------------------------------------------------------- |
| **Serialización**             | El código tiene que viajar por red desde el driver hasta cada worker                   |
| **Estado mutable compartido** | Spark puede re-ejecutar tareas y múltiples workers operan en paralelo sin coordinación |
| **Efectos secundarios**       | La evaluación lazy y el no-determinismo en el orden de ejecución                       |

Broadcast variables y accumulators son la superficie controlada que Spark expone para los únicos dos patrones de estado compartido que puede garantizar correctamente: **lectura eficiente de datos grandes** y **acumulación de resultados parciales**.

## Ejercicio 2 — Paralelizar la descarga de feeds

**¿Qué pasaría si dejáramos propagar la excepción del flatMap?**
Si dejáramos propagar dicha excepción, rompería el concepto de resiliencia que posee
nuestro programa, pues al "explotar" por al menos un feed mal descargado, interrumpiría
la ejecución completa, incluso si hay workers que han podido descargar sus feeds correctamente.

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

- **reduceByKey es una barrera de sincronización. ¿Qué ocurre en el cluster en ese punto? ¿Por qué es inevitable para este problema?**

**reduceByKey** es una barrera de sincronización, pues necesita: por un lado debe agrupar los mismos valores asociados a una misma clave, para luego realizar la reducción mediante dicha agrupación en cada Worker.

Internamente en el Cluster, ocurre un fenómeno conocido como **Shuffle**, el cuál consiste en un intercambio de datos entre diferentes Workers mediante **Stages**:

- **Stage 1 (local):** Cada worker procesa su partición y aplica una reducción parcial sobre sus propias claves. Esto es una optimización que Spark hace automáticamente: en lugar de mandar todos los datos crudos, manda resultados parciales.

- **Shuffle — redistribución por clave:** Los datos se escriben a disco (shuffle write), se transfieren por red, y los workers de destino los leen (shuffle read). Todas las ocurrencias de una misma clave deben llegar al mismo worker.

- **Stage 2 — reduce final:** Cada worker recibe todos los valores para sus claves asignadas y aplica la reducción final.

**La barrera de sincronización está entre Stages:** ningún worker puede empezar el Stage 2 hasta que todos los workers terminaron el Stage 1, porque necesitan los datos del shuffle completo.

**Es inevitable** dado que:

Para reducir por clave, necesitás que **todos los valores** de esa clave estén en el mismo lugar. Dado que los datos están **distribuidos arbitrariamente entre particiones**, no hay forma de garantizar eso sin mover datos entre workers.

- **¿Qué restricciones debe cumplir la función que se le pasa a reduceByKey? Piensen en conmutatividad y asociatividad.**

La función debe ser **asociativa** y **conmutativa**.
**Asociativa** porque Spark divide los datos entre workers y cada uno produce un resultado parcial. Luego Spark combina esos parciales sin garantizar en qué orden. Si la función no fuera asociativa, `(a op b) op c` podría dar distinto que `a op (b op c)` y el resultado final dependería de cómo Spark agrupó los parciales — lo cual es no determinístico.

**Conmutativa** porque dentro de cada worker los elementos de una partición pueden llegar en cualquier orden. Si la función no fuera conmutativa, `a op b` podría dar distinto que `b op a`.
La suma cumple ambas: `(2 + 3) + 4 == 2 + (3 + 4)` y `2 + 3 == 3 + 2` al igual que la multiplicación. La resta no cumple ninguna: `(5 - 3) - 1 != 5 - (3 - 1)` y `5 - 3 != 3 - 5`.

- **¿Dónde se hace la lectura del diccionario de entidades? ¿En el driver o los workers?**

La lectura la realiza el **driver**, una sola vez antes de que comience el pipeline distribuido. Una vez cargado, se envía a los workers como una **broadcast variable**, lo que significa que Spark lo serializa y manda **una copia por worker** en lugar de una copia por tarea. Esto reduce significativamente el tráfico de red — sin broadcast, Spark enviaría el dictionary completo con cada tarea individual que lo necesite. Adicionalmente, para que Spark pueda serializar el dictionary, la clase `NamedEntity` debe implementar el trait `Serializable` — sin esto el pipeline falla al intentar distribuir el objeto a los workers.

## Ejercicio 5 — Acceso a datos y estadísticas del resultado

a) En Main.scala no se utiliza .cache() ni .unpersist() sobre los RDD intermedios. Por eso, cada vez que se ejecuta una accion, spark vuelve a calcular todas las transformaciones desde el principio

Esto ocurre, por ejemplo, cuando se ejecutan allPostsRDD.count(), filteredPostsRDD.count() y el calculo de totalChars. En cada caso Spark vuelve a descargar los feeds, parsear los JSON y aplicar los filtros

Algo similar sucede al obtener los resultados del NER mediante collect(). Como los RDD no estan almacenados en memoria, Spark vuelve a recorrer todo el pipeline y ejecuta nuevamente Analyzer.detectEntities() para cada post válido. Dado que esta es una de las operaciones mas costosas del programa, se produce un gasto innecesario de tiempo y recursos

---

- **¿Que ocurriria si no llamaran a cache()? ¿Cuantas veces se ejecutaria la descarga de feeds?**

Si no usaramos cache(), por la forma en que funciona Spark (lazy evaluation), cada vez que se ejecuta una accion como count(), reduce() o collect(), Spark tiene que volver a recorrer todas las transformaciones desde el principio. En este caso, eso incluye volver a descargar los feeds.

Mirando el codigo, hay 5 acciones que dependen de esos datos:

1. allPostsRDD.count()
2. filteredPostsRDD.count()
3. reducedEntities.collect()
4. entities.collect()

Entonces, si no estuviera cache(), la descarga completa de feeds se haria 5 veces, una por cada accion. Como descargar los feeds es una operacion bastante costosa, el tiempo de ejecucion aumentaria mucho.

- **¿Por que es incorrecto llamar a collect() entre los pasos a) y b) del ejercicio 3 y luego continuar el pipeline? ¿Que consecuencia tiene sobre la distribucion del trabajo?**

Llamar a collect() hace que todos los datos que estan repartidos entre los workers se envien al driver. Si hacemos eso despues del paso a), donde ya tenemos todas las entidades extraidas, estariamos moviendo una gran cantidad de datos por la red hacia una sola maquina.

Esto puede generar problemas de memoria en el driver e incluso provocar un error por falta de memoria. Ademas, a partir de ese momento el procesamiento dejaria de hacerse de forma distribuida y pasaria a ejecutarse en una sola maquina, perdiendo una de las principales ventajas de Spark.

Si despues quisieramos volver a distribuir esos datos, tendriamos que usar parallelize, lo que implicaria enviarlos nuevamente a los workers y agregaria un costo extra totalmente innecesario.

- **cache() es tambien lazy. ¿En que momento se almacena realmente el RDD en memoria?**

cache() tambien es una operacion lazy, asi que cuando la escribimos no guarda nada inmediatamente en memoria. Lo unico que hace es marcar el RDD para que sea almacenado cuando se necesite.

El RDD se guarda realmente la primera vez que una accion obliga a calcularlo. Mientras Spark va recorriendo las particiones para ejecutar esa accion, aprovecha y las va almacenando en memoria. A partir de ese momento, las siguientes acciones pueden reutilizar esos datos sin tener que recalcular todo desde cero.
