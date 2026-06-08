import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

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

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = subscriptions.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
      (feedOpt.isDefined, posts)
    }

    // Count feed successes/failures
    val feedsSuccess = downloadResults.count(_._1)
    val feedsFailed = downloadResults.length - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val allPosts = downloadResults.flatMap(_._2)
    val postsSuccess = allPosts.length
    val postsFailed = downloadResults.count(_._2.isEmpty)

    // Filter empty posts
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts)
    val postsFiltered = allPosts.length - filteredPosts.length

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
