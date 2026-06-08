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
* count
* collect
* foreach
* print

### Transformaciones
Son las diferentes etapas por las cuales se va pasando en el flujo del Pipeline, estos no tienen resultados inmediatos y solo son pasos iniciales, intermedios y finales para llegar de un **"Input General"** hasta el **"Output General"**.
Toman un RDD y devuelven otro RDD. **Son Lazy**, no se ejecutan hasta que alguien las necesita.

**Ejemplos:**
* map 
* flatMap
* reduceByKey


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

* 2 -> **flatMap**: Por cada subscripción puede devolver **VARIOS o NINGÚN feed**

* 3 -> **flatMap**: Por cada feed puede devuelver **VARIOS o NINGÚN post**. 

* 4 -> **"filter"**: Si bien no está explícito en la consigna, vale la pena mencionarlo ya que sacamos posts vacíos, si lo queremos ver a modo de **flatMap**, donde puede devolver 1 o 0 elementos (no usamos **map** pues debería consistir en que devuelva EXACTAMENTE un solo elemento).

* 5 -> **flatMap**: Por cada Post pueden surgir **VARIAS o NINGUNA entidad**.

* 6 -> **map**: Por cada entidad lo mapeamos a una **ÚNICA TUPLA (Clave,Valor)**.

* 7 -> **reduceByKey u otras reducciones**: Por cada entidad se organiza en base a una **Clave** y se colapsan los **Valores**.

### Lo que NO se puede representar como una Abstracción de Spark

* 1 -> **Es el Driver**: Leer subscripciones es "preparar el terreno" para poder trabajar con paralelismo con Spark. Descartamos las 3 opciones pues:
  * **map y flatMap**: Necesitamos como input un **RDD**, y como input tenemos un **String**.
  * **Reducciones**: necesita pares clave-valor distribuidos para combinar.

  Y una pista clave la encontramos en el Ejercicio 2 cuando nos dicen:

  *"Lean las suscripciones de subscriptions.json y cárguenlas en un RDD con
  sc.parallelize(subscriptions)"*.

* 8 -> **Es el Driver (Ranking e Impresiones en pantalla)**: Es la etapa final del programa. El ordenamiento descendente del ranking y el formateo de texto son procesos secuenciales que se ejecutan de manera central en el Driver. No pueden ser transformaciones de Spark porque no generan un nuevo RDD, sino que consumen el resultado final distribuido mediante **acciones (como collect)** para generar un **efecto secundario (la salida en consola)**.
  
### c) Barreras de sincronización y paralelismo

#### partes completamentes independientes son : 

- FileIO.downloadFeed(...)
- JsonParser.parsePosts(...)
- Analizer.filterEmptyPosts(...)
- Analyzer.detectEntities(...)

#### partes que son barreras de sincronización (requiere que todos los workers terminen) :

- downloadResults.count(...)
- if (filteredPosts.isEmpty) - verificación de post vacios 
- Analizer.countEntities
- Analizer.countByType
- .sortBy{...}

Los pasos de descarga, parseo, filtrado y detección de entidades son completamente independientes, cada worker opera sobre su porción de datos sin coordinarse con otros, y el resultado de uno no afecta al de los demás.
Las barreras aparecen exactamente donde el resultado depende de la colección completa, cálculo de estadísticas globales (conteos, promedios), el chequeo de isEmpty, y las dos agregaciones por clave (countEntities y countByType). En estas etapas Spark ejecuta un shuffle, redistribuye los datos entre workers según la clave, y ningún worker puede producir su parte del resultado final hasta que todos hayan terminado la fase anterior.
