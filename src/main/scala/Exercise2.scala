import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Exercise2 {

  /**
   * This function shows the 5 most used words in tweets for a specific sentiment of airline.
   * It first splits each tweet into individual words. Then, it groups
   * the DataFrame by airline sentiment and word, counting the number of occurrences for
   * each word under the specified sentiment. The count of words is sorted in
   * descending order to show the most used words first. The top 5 words are then
   * displayed.
   * Source idea -> https://sparkbyexamples.com/spark/convert-delimiter-separated-string-to-array-column-in-spark/
   */
  def show5MostUsedWords(df: DataFrame, airline_sentiment: String): Unit = {
    println("5 most used words in " + airline_sentiment + " airline sentiment text:")

    // Split column text into words, explode result arrays of words into new rows, give name "word" to the result column
    df.withColumn("word", explode(split(col("text"), " ")))
      // Group by column word and counts the number of occurrences of each word
      .groupBy("airline_sentiment", "word").count()
      // Case of specific airline_sentiment, given as argument of the method, and also avoid empty strings
      .where("airline_sentiment = airline_sentiment AND word!=''")
      // Sort in descending order by the count
      .sort(desc("count"))
      // Limit to 5 first rows
      .limit(5)
      // Print result
      .show()
  }

  def main(args: Array[String]): Unit = {

    // Create the spark session first
    val spark = SparkSession.builder()
      .master("local")
      .appName("Exercise2")
      .getOrCreate()

    // Set filename of the csv file
    val fileName = "src/main/resources/tweets.csv"
    // Read the contents of the csv file in a dataframe. The csv file contains a header.
    val basicDF = spark.read.option("header", "true").csv(fileName)

    // Convert columns to the appropriate data type.
    val modifiedDF = basicDF
      .withColumn("airline_sentiment_confidence", col("airline_sentiment_confidence").cast("double"))
      .withColumn("negativereason_confidence", col("negativereason_confidence").cast("double"))
      .withColumn("tweet_created", col("tweet_created").cast("timestamp"))

    // Remove symbols from data in column text
    val noSymbolsModifiedDF = modifiedDF.withColumn("text", regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", ""))
    // Convert all chars of column text to lower case
    val lowerCaseModifiedDF = noSymbolsModifiedDF.withColumn("text", lower(col("text")))

    // Question 1
    // Print the results
    // Case of airline_sentiment = 'positive'
    show5MostUsedWords(lowerCaseModifiedDF, "positive")
    // Case of airline_sentiment = 'neutral'
    show5MostUsedWords(lowerCaseModifiedDF, "neutral")
    // Case of airline_sentiment = 'negative'
    show5MostUsedWords(lowerCaseModifiedDF, "negative")

    // Question 2
    // Create DataFrame mainCauseOfComplaintsPerAirlineDF by selecting
    // the columns "airline" and "negativereason" from lowerCaseModifiedDF
    val mainCauseOfComplaintsPerAirlineDF = lowerCaseModifiedDF
      .select("airline", "negativereason")
      // Filter rows where "negativereason_confidence" is greater than 0.5
      .where("negativereason_confidence > 0.5")
      // Group data by "airline" and "negativereason"
      .groupBy("airline", "negativereason")
      // Count the number of occurrences of each "negativereason"
      // per "airline" and rename the new column as "count"
      .agg(count("negativereason").as("count"))

    // Create a DataFrame that contains the maximum number of complaints per airline
    val maxComplaintsDF = mainCauseOfComplaintsPerAirlineDF
      // Group data by "airline"
      .groupBy("airline")
      // For each "airline", find the "negativereason" with the maximum count.
      // The resulting struct column is renamed as "max".
      .agg(max(struct("count", "negativereason")).as("max"))
      // Extract the "airline" and "negativereason" from the "max" struct column
      .select("airline", "max.negativereason")

    // Print results
    println("Main cause of complaints per Airline:")
    maxComplaintsDF.show()

    spark.stop()
  }
}


