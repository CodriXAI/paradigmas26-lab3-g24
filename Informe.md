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