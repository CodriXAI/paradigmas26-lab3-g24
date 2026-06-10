import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

    // ==========================================
    // EJERCICIO 2 - INCISO A
    // ==========================================
    // Configuration and start of the Spark Session
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Temporarily disable noisy Spark logs
    spark.sparkContext.setLogLevel("OFF")

    // Loading and initializing subscriptions in the Driver
    val subscriptions = FileIO.readSubscriptions(cmdArgs.subscriptionFile) match {
      case None => spark.stop(); return
      case Some(list) => list.flatten
    }

    // Validation: if there are no valid subscriptions, stop the program
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }

    // Parallelize subscriptions into RDD
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // ==========================================
    // EJERCICIO 2 - INCISO B
    // ==========================================
    // Download feeds and parse posts, tracking success/failure
    val allPostsRDD = subscriptionsRDD.flatMap { subscription =>
      try{
        FileIO.downloadFeed(subscription) match {
          case None =>
            // In case of a download error, a warning is logged and an empty list of posts 
            // for that subscription is returned.
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
            List[Post]()
          case Some(content) =>
            // parsePost handles parse error 7 case.
            val parsedPosts = JsonParser.parsePosts(content, subscription.name)
            parsedPosts
        }
      } catch {
        case e: Exception =>
          // Internal fault handling so that an error does not cancel all processing
          println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
          List[Post]()
      }
    }

    val filteredPostsRDD = allPostsRDD.filter(post =>
      post.title.nonEmpty && post.selftext.nonEmpty
    )

    // ==========================================
    // EJERCICIO 2 - INCISO C
    // ==========================================
    // Prints how many posts were downloaded, how many were filtered,
    // and the average length in characters of the filtered posts. The
    // other stats will be completed in exercise 4.
    val totalPosts = allPostsRDD.count().toInt
    val totalFilteredPosts = filteredPostsRDD.count().toInt
    val emptyPosts = totalPosts - totalFilteredPosts

    // ==========================================
    // EJERCICIO 2 - INCISO D
    // ==========================================
    // Check if there are no posts after the filter
    if(totalFilteredPosts == 0){
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    val totalChars = filteredPostsRDD.map{ post =>
      post.title.length + post.selftext.length
    }.reduce((a, b) => a + b)

    val avgChars = (totalChars / totalFilteredPosts).toInt

    val stats = Map(
      "feedsSuccess"  -> 0, // will be replaced in exercise 4 with accumulators
      "feedsFailed"   -> 0, // will be replaced in exercise 4 with accumulators
      "postsSuccess"  -> totalPosts,
      "postsFailed"   -> 0, // will be replaced in exercise 4 with accumulators
      "postsFiltered" -> emptyPosts,
      "avgChars"      -> avgChars
    )

    println(Formatters.formatProcessingStats(stats))

    // ==========================================
    // EJERCICIO 3 - INCISO A
    // ==========================================
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)
    if (dictionary.isEmpty){
      println("Error: entities dictionary empty")
      spark.stop()
      return
    }

    val dictionaryBCast = sc.broadcast(dictionary)  

    val entities = filteredPostsRDD.flatMap{ post =>
      val text = post.title + " " + post.selftext

      Analyzer.detectEntities(text, dictionaryBCast.value)
    }

    // ==========================================
    // EJERCICIO 3 - INCISO B
    // ==========================================
    val mappedEntities = entities.map{ e =>
      ((e.entityType, e.text), 1)
    }
    
    // ==========================================
    // EJERCICIO 3 - INCISO C
    // ==========================================
    val reducedEntities = mappedEntities.reduceByKey(_ + _)

    // ==========================================
    // EJERCICIO 3 - INCISO D
    // ==========================================
    val entityCounts = reducedEntities.collect().toMap 

    val entitiesStats = entities.collect().toList

    val typeStats = Analyzer.countByType(entitiesStats)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}
