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
    // EJERCICIO 4 - INCISO A
    // ==========================================
    // Accumulators for tracking feed and post processing statistics and 
    // metrics globally across the cluster
    // only the driver can read them; the workers can only increment them
    // - Metrics related to feed downloading success/failure
    val accFeedsSuccess  = sc.longAccumulator("feedsSuccess")
    val accFeedsFailed   = sc.longAccumulator("feedsFailed")
    // - Metrics related to post processing (total posts, filtered posts)
    val accPostsTotal    = sc.longAccumulator("postsTotal")
    val accPostsFiltered = sc.longAccumulator("postsFiltered")

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
            accFeedsFailed.add(1)
            List[Post]()
          case Some(content) =>
            accFeedsSuccess.add(1)
            // parsePost handles parse error 7 case.
            val parsedPosts = JsonParser.parsePosts(content, subscription.name)
            accPostsTotal.add(parsedPosts.length)
            parsedPosts
        }
      } catch {
        case e: Exception =>
          // Internal fault handling so that an error does not cancel all processing
          println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
          accFeedsFailed.add(1)
          List[Post]()
      }
    }

    // Changed from .filter() to .flatMap() to allow incrementing accPostsFiltered
    // within the transformation (workers can only write Accumulators).
    val filteredPostsRDD = allPostsRDD.flatMap { post =>
      if (post.title.nonEmpty && post.selftext.nonEmpty) {
        List(post)
      } else {
        accPostsFiltered.add(1)
        List.empty[Post]
      }
    }

    // ==========================================
    // EJERCICIO 2 - INCISO C (ANTES) 
    // EJERCICIO 4 - INCISO C (AHORA)
    // ==========================================
    // Timing the download and filtering phase using System.currentTimeMillis()
    val t0Download = System.currentTimeMillis()

    // Using aggregate to compute total filtered posts and total characters in one pass,
    // which allows us to read the accumulators only once after the action is triggered.
    val (totalFilteredPosts, totalCharsAgg) = filteredPostsRDD.aggregate((0L, 0L))(
      seqOp  = (acc, post) => (acc._1 + 1L, acc._2 + post.title.length + post.selftext.length),
      combOp = (a, b)      => (a._1 + b._1,  a._2 + b._2)
    )

    // Timing the end of the download and filtering phase and printing the elapsed time
    val t1Download = System.currentTimeMillis()
    println(s"[Time] Download and filtering: ${(t1Download - t0Download) / 1000.0} s")

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

    // ==========================================
    // EJERCICIO 4 - INCISO B
    // ==========================================
    // Stats calculation for feed and post processing, using accumulators 
    // to track counts across the cluster
    val stats = Map(
      "feedsSuccess"  -> accFeedsSuccess.value.toInt,
      "feedsFailed"   -> accFeedsFailed.value.toInt,
      "postsSuccess"  -> accPostsTotal.value.toInt,
      "postsFailed"   -> 0,
      "postsFiltered" -> accPostsFiltered.value.toInt,
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
    // EJERCICIO 3 - INCISO D (ANTES)
    // EJERCICIO 4 - INCISO C (AHORA)
    // ==========================================
    // Measurement (NER + count)
    // Timing the NER phase using System.currentTimeMillis()
    val t0Ner = System.currentTimeMillis()

    val entityCounts = reducedEntities.collect().toMap 

    // Timing the end of the NER phase and printing the elapsed time
    val t1Ner = System.currentTimeMillis()
    println(s"[Time] NER and entity counting: ${(t1Ner - t0Ner) / 1000.0} s")

    // Deriving typeStats from entityCounts avoids calling entities.collect()
    // which would be a second action on the same lineage and would re-evaluate
    // filteredPostsRDD from scratch (a problem that cache() will solve in Exercise 5).
    val total  = entityCounts.values.sum
    val byType = entityCounts
      .groupBy { case ((entityType, _), _) => entityType }
      .view
      .mapValues { group => group.values.sum }
      .toMap
    val typeStats = byType + ("total" -> total)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    println("=== PROGRAMA PAUSADO: Abrí http://localhost:4040 en tu navegador para ver la Spark UI ===")
    scala.io.StdIn.readLine("Presioná Enter para finalizar y cerrar Spark...")

    spark.stop()
  }
}
