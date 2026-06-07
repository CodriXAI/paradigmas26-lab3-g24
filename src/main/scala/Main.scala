import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // 1. Parseo de argumentos de la línea de comandos
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

    // 2. Configuración e inicio de la Spark Session (Prerrequisito)
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]") // Modo local usando todos los cores
      .getOrCreate()
    val sc = spark.sparkContext

    // Desactivamos temporalmente los logs ruidosos de Spark
    spark.sparkContext.setLogLevel("OFF")

    // 3. Carga e inicialización de las suscripciones en el Driver
    val subscriptions = FileIO.readSubscriptions(cmdArgs.subscriptionFile) match {
      case None => spark.stop(); return
      case Some(list) => list.flatten
    }

    // Validación: si no hay suscripciones válidas, frena el programa
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }

    // ==========================================
    // EJERCICIO 2 - INCISO A
    // ==========================================
    // Paraleliza las suscripciones leídas en un RDD distribuido
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // ==========================================
    // EJERCICIO 2 - INCISO B
    // ==========================================
    // Un solo flatMap distributivo que descarga, maneja excepciones y
    // devuelve los posts ya filtrados (title y selftext no vacíos)
    val postsRDD = subscriptionsRDD.flatMap { subscription =>
      try {
        FileIO.downloadFeed(subscription) match {
          case Some(content) => 
            // Parsea los posts crudos del feed JSON
            val parsedPosts = JsonParser.parsePosts(content, subscription.name)
            
            // Filtramos in-place: solo posts con título y cuerpo válidos
            parsedPosts.filter { post =>
              post.title.nonEmpty && post.selftext.nonEmpty && post.selftext.trim.nonEmpty
            }
          case None =>
            // Si downloadFeed devuelve None, reporta el warning y devuelve lista vacía
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
            List[Post]()
        }
      } catch {
        case e: Exception =>
          // Manejo interno contra fallos para que un error no cancele todo el procesamiento
          println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
          List[Post]()
      }
    }

    // ==========================================
    // ACCIÓN TEMPORAL DE PRUEBA
    // ==========================================
    // Como los RDDs son lazy, agregamos un .count() para forzar a Spark a ejecutar 
    // el pipeline distribuido y verificar cuántos posts válidos se obtuvieron.
    val totalPostsValidos = postsRDD.count()
    println(s"Prueba provisoria: Se procesaron con éxito $totalPostsValidos posts filtrados.")

    // Cierre limpio de la sesión de Spark al finalizar
    spark.stop()
  }
}