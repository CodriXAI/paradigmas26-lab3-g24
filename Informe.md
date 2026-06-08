# Informe - Laboratorio 3: Procesamiento Distribuido con Apache Spark

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo y tipos en Scala

El pipeline del programa sigue la siguiente secuencia de pasos:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FLUJO DEL PIPELINE REDDIT NER                        │
└─────────────────────────────────────────────────────────────────────────┘

DRIVER (lectura inicial)
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 1: Lectura de suscripciones                                        │
│ ────────────────────────────────────                                    │
│ Input:  subscriptions.json (archivo)                                    │
│ Output: List[Subscription]                                              │
│ Tipo:   List[Subscription]                                              │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 2: Descarga de feeds (PARALELIZABLE)                               │
│ ────────────────────────────────────────────                            │
│ Input:  List[Subscription]                                              │
│ Output: List[(Option[String], Subscription)]                            │
│         (contiene el JSON del feed o None si falla)                     │
│ Tipo:   List[(Option[String], Subscription)]                            │
│ Esta etapa es INDEPENDIENTE por cada suscripción.                       │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 3: Parseo de posts JSON (PARALELIZABLE)                            │
│ ─────────────────────────────────────                                   │
│ Input:  List[(Option[String], Subscription)]                            │
│ Output: List[Post]                                                      │
│ Tipo:   List[Post]                                                      │
│ Cada feed se parsea de forma independiente.                             │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 4: Filtrado de posts vacíos (PARALELIZABLE)                        │
│ ──────────────────────────────────────────                              │
│ Input:  List[Post]                                                      │
│ Output: List[Post] (solo posts con title y selftext no vacíos)          │
│ Tipo:   List[Post]                                                      │
│ Cada post se evalúa de forma independiente.                             │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 5: Extracción de entidades nombradas (PARALELIZABLE)              │
│ ─────────────────────────────────────────────                           │
│ Input:  List[Post]                                                      │
│ Output: List[NamedEntity]                                               │
│ Tipo:   List[NamedEntity]                                               │
│ Cada post se procesa de forma independiente.                            │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 6: Convertir a pares (tipo, nombre) → 1 (PARALELIZABLE)           │
│ ────────────────────────────────────────────────────────────            │
│ Input:  List[NamedEntity]                                               │
│ Output: List[((String, String), Int)]                                   │
│         Donde el Int siempre es 1 en esta fase (map)                    │
│ Tipo:   List[((String, String), Int)]                                   │
│ Cada entidad se transforma de forma independiente.                      │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 7: Conteo de entidades - reduceByKey (BARRERA)                    │
│ ───────────────────────────────────────────────                         │
│ Input:  List[((String, String), Int)]                                   │
│ Output: List[((String, String), Int)]                                   │
│         Donde Int es el conteo total por entidad                        │
│ Tipo:   List[((String, String), Int)]                                   │
│ ⚠️  BARRERA DE SINCRONIZACIÓN - todos los workers deben terminar       │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PASO 8: Ranking y formateo (DRIVER)                                     │
│ ───────────────────────────────                                         │
│ Input:  List[((String, String), Int)]                                   │
│ Output: String (salida formateada por pantalla)                         │
│ Tipo:   String                                                          │
│ Se ordena por conteo descendente y tipo alfabético.                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Tabla resumen de tipos Scala por conexión:

| De → A | Paso | Tipo Input | Tipo Output |
|--------|------|-----------|------------|
| DRIVER → Lectura | 1 | `String` (archivo) | `List[Subscription]` |
| 1 → 2 | Descarga feeds | `List[Subscription]` | `List[(Option[String], Subscription)]` |
| 2 → 3 | Parseo | `List[(Option[String], Subscription)]` | `List[Post]` |
| 3 → 4 | Filtrado | `List[Post]` | `List[Post]` |
| 4 → 5 | Extracción NER | `List[Post]` | `List[NamedEntity]` |
| 5 → 6 | Conversión a pares | `List[NamedEntity]` | `List[((String, String), Int)]` |
| 6 → 7 | Conteo (reduce) | `List[((String, String), Int)]` | `List[((String, String), Int)]` |
| 7 → 8 | Ranking | `List[((String, String), Int)]` | `String` (salida) |

---

## Ejercicio 2 — Paralelizar la descarga de feeds

*(Pendiente de completar)*

---

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

*(Pendiente de completar)*

---

## Ejercicio 4 — Monitoreo del éxito de las tareas

*(Pendiente de completar)*

---

## Ejercicio 5 — Acceso a datos y estadísticas del resultado

*(Pendiente de completar)*

---

## Decisiones de diseño

*(A completar una vez implementada la solución con Spark)*

---

## Respuesta a preguntas conceptuales

*(A completar con análisis de Spark, barreras de sincronización, restricciones de serialización, etc.)*
