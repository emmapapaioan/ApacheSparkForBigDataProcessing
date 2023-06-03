import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc, explode, lower, regexp_extract, regexp_replace, split}

object Exercise3 {

  /**
   * This function splits a specified column by a given delimiter,
   * explodes it into separate rows, groups the DataFrame by the
   * resulting column, counts the occurrences per group,
   * and returns the resulting DataFrame.
   */
  def groupByAndCount(df: DataFrame, delimiter: String, outColName: String, countColName: String): DataFrame = {
    // Split the countColName column by the given delimiter and explode it
    // into separate rows to create a new column named with value of outColName.
    df.withColumn(outColName, explode(split(col(countColName), delimiter)))
      // Group results by outColName column.
      .groupBy(outColName)
      // Count occurrences per group.
      .count()
      // Sort in ascending order by the outColName.
      .sort(asc(outColName))
  }

  /**
   * This function takes as input a DataFrame and two column names (titleCol and yearCol).
   * It extracts a 4-digit year from the titleCol, groups the DataFrame by the extracted year,
   * counts the number of titles for each year, and sorts these counts in descending order.
   * The result is a DataFrame with years and their counts.
   */
  def countMoviesPerYear(df: DataFrame, titleCol: String, yearCol: String): DataFrame = {
    df.select(
      // Extract the year from the titleCol column using a regex to find 4 digit numbers enclosed in parentheses
      regexp_extract(col(titleCol), "\\((\\d{4})\\)", 1)
        // Rename the resulting column to the name provided in yearCol
        .alias(yearCol))
      // Group the DataFrame by the yearCol column, which now contains the extracted years
      .groupBy(yearCol)
      // Count the number of titles per year
      .count()
      // Sort the results by the count in descending order
      .sort(desc("count"))
  }

  /**
   * This function takes a DataFrame df and an integer occurrences as parameters.
   * It splits the "title" column into individual words, removes symbols and numbers from the words,
   * converts them to lowercase, and then groups the words by count. The function filters the result
   * based on the specified occurrences, excluding empty strings and words with a length less than 4 characters.
   * Finally, the result is sorted in descending order based on the count column.
   */
  def countWordsWithSpecificOccurrences(df: DataFrame, occurrences: Int): DataFrame = {
    // Split column "title" into words, explode result arrays of words into new rows, give name "word" to the result column
    df.withColumn("word", explode(split(col("title"), " ")))
      // Remove numbers and symbols in column "word"
      .withColumn("word", regexp_replace(col("word"), "[^a-zA-Z]", ""))
      // Convert to lower case, the elements of column "word"
      .withColumn("word", lower(col("word")))
      // Group by column word and count the number of occurrences of each word
      .groupBy( "word").count()
      // Case of specific occurrences, given as argument of the method, and also avoid empty strings
      .where(s"count >= " + occurrences + " AND word != '' AND length(word) >= 4")
      // Sort in descending order by the count
      .sort(desc("count"))
  }

  def main(args: Array[String]): Unit = {
    // Create the spark session first
    val spark = SparkSession.builder()
      .master("local")
      .appName("Exercise3")
      .getOrCreate()

    // Set filename of the csv file
    val fileName = "src/main/resources/movies.csv"
    // Read the contents of the csv file in a dataframe. The csv file contains a header.
    val moviesDF = spark.read.option("header", "true").csv(fileName)

    // Question 1: Count movies per genre
    println("Count movies per genre:")
    groupByAndCount(moviesDF, "\\|", "genre", "genres").show()

    // Question 2: Top 10 - Count movies per year
    println("Top 10 - Count movies per year:")
    countMoviesPerYear(moviesDF, "title", "year").limit(10).show();

    // Question 3: Words with at least 10 occurrences at movie titles and their total occurrences
    println("Words, in movie titles, with at least 10 occurrences:")
    countWordsWithSpecificOccurrences(moviesDF, 10).show()

    spark.stop();
  }
}
