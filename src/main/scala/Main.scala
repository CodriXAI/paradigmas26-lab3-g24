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
    
    spark.stop()
  }
}
