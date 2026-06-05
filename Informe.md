# LAB 3 : Procesamiento distribuido con Apache Spark

## Integrantes grupo 24:

- Altube, Alfredo
- Ampuero, Fernando
- Colares, Cristian
- Huaman, Alexander 

## Ejercicio 1 - Preguntas 

A.  Dibujen el diagrama de flujo de los pasos que tiene que hacer su programa (conexión, descarga, extracción de entidades, clasificación, conteo, ranking) como un grafo de dependencias (que seguramente será algo muy parecido a una secuencia).
Cada uno de los pasos será una acción o transformación que realiza un worker o el driver. La conexión entre un paso A y un paso B es el output de A y el input de B. Explicite el tipo en Scala de cada conexión.

Respuesta (Borrar esta linea): ... 


B.  Para cada paso del pipeline, determinen si puede expresarse como una de las abstracciones de Spark:
- map: transforma cada elemento en exactamente un resultado. Aplicable cuando cada tarea es independiente y produce exactamente una salida.
- flatMap: transforma cada elemento en cero o más resultados. Aplicable cuando cada tarea es independiente pero puede producir una cantidad variable de salidas.
- reduceByKey u otra reducción: combina múltiples elementos en uno agrupando por clave. Aplicable cuando el resultado depende de todos los elementos, no de uno solo.
¿Hay algún paso del pipeline que no encaje en ninguna de estas abstracciones? ¿Por qué?

Respuesta (Borrar esta linea): ...  

C.  Las reducciones constituyen una barrera de sincronización: ningún worker puede producir el resultado final hasta que todos hayan terminado su parte. Identifiquen qué
pasos del pipeline son barreras y cuáles pueden ejecutarse de forma completamente independiente entre workers.

partes completamentes independientes son : 

- FileIO.downloadFeed(...)
- JsonParser.parsePosts(...)
- Analizer.filterEmptyPosts(...)
- Analyzer.detectEntities(...)

partes que son barreras de sincronización (requiere que todos los workers terminen) :

- downloadResults.count(...)
- if (filteredPosts.isEmpty) - verificación de post vacios 
- Analizer.countEntities
- Analizer.countByType
- .sortBy{...}

Los pasos de descarga, parseo, filtrado y detección de entidades son completamente independientes, cada worker opera sobre su porción de datos sin coordinarse con otros, y el resultado de uno no afecta al de los demás.
Las barreras aparecen exactamente donde el resultado depende de la colección completa, cálculo de estadísticas globales (conteos, promedios), el chequeo de isEmpty, y las dos agregaciones por clave (countEntities y countByType). En estas etapas Spark ejecuta un shuffle, redistribuye los datos entre workers según la clave, y ningún worker puede producir su parte del resultado final hasta que todos hayan terminado la fase anterior.

D.  El mecanismo de extensión (extension point) de Spark es la función que el desarrollador le pasa a cada transformación. ¿Qué restricciones impone Spark sobre None esas funciones para que puedan ejecutarse en un entorno distribuido? Piensen en serialización, estado compartido y efectos secundarios.

Respuesta (Borrar esta linea): ...  