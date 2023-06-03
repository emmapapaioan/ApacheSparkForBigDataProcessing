import org.apache.spark.sql.SparkSession

import java.io.{FileNotFoundException, IOException}

object Exercise1 {

  /**
   * This function creates an array of all the words contained in a given string as argument
   * The function also converts chars to lower case, removes all symbols and finally removes
   * all the words starting with number
   */
  def processLineOfText(line: String): Array[String] = {
    // Convert all chars of the string to lower case
    line.toLowerCase()
      // Split the words of each line and create an array of words
      .split(" ")
      // Remove all symbols
      .map(word => word.replaceAll("[^a-zA-Z]", ""))
      // Remove all words starting with number
      .filterNot(word => word.headOption.exists(_.isDigit))
      // Ignore the empty words (e.g. by continuous white space)
      .filter(_.nonEmpty)
  }

  /**
   * This function rounds up a double number to
   * n decimal places
   */
  def round(number: Double, n: Int): Double = {
    BigDecimal(number).setScale(n, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def main(args: Array[String]): Unit = {
    // Start time
    val startTime = System.nanoTime()

    // Create the spark session first
    val spark = SparkSession.builder()
      .master("local")
      .appName("Exercise1")
      .getOrCreate()

    val fileName = "src/main/resources/SherlockHolmes.txt"

    try {
      // Read a file and create an RDD
      val rdd = spark.sparkContext.textFile(fileName)
      // For each line of the text file, retrieve the words
      // edit them and put them in an array
      val wordsRdd = rdd.flatMap(line => processLineOfText(line))
      //  Map the wordsRdd into an RDD of pairs consisting of
      // the first letter of each word and the length of each word
      val wordPairs = wordsRdd.map(word => (word(0), word.length))
      // Calculate total word lengths and counts per letter
      val totalLettersCount = wordPairs
        .mapValues(length => (length, 1))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      // Calculate average length for words starting with specific letter
      val letterAvgLength = totalLettersCount
        // For each letter, calculate the avg word length, rounded up to 2 decimal places
        .mapValues { case (sum, count) => round((sum.toDouble / count), 2) }
        // Collect results from the RDD
        .collect()
        // Convert to list
        .toList
        // Sort in descending order by the count avg word length
        .sortWith((a, b) => a._2 > b._2)

      // Print the result list
      println("Average word length for each starting letter:")
      letterAvgLength.foreach { case (letter, avgLength) =>
        println(letter + ": " + "%.2f".format(avgLength))
      }
    } catch {
      case e: FileNotFoundException => println("Error occurred while trying find file at the directory " + fileName + ".")
      case e: IOException => println("Error occurred while trying find file at the directory " + fileName + ". Make sure the directory is setted correctly.")
      case e: IllegalArgumentException => println("Invalid argument provided: " + e.getMessage)
      case e: SecurityException => println("Security exception occurred: " + e.getMessage)
      case e: Exception => println("An unexpected error occurred: " + e.getMessage)
    } finally {
      // End the spark session
      spark.stop()

      // End time
      val endTime = System.nanoTime()

      // Execution time
      val duration = (endTime - startTime) / 1e9d
      print("\nExecution time: " + round(duration, 2) + " sec")
    }
  }
}