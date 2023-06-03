import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Exercise4 {

  def main(args: Array[String]): Unit = {
    // Create the spark session first
    val spark = SparkSession.builder()
      .master("local")
      .appName("Exercise4")
      .getOrCreate()

    val fileName = "src/main/resources/web-Stanford.txt"

    // A much simpler way to build a graph
    //val graph = GraphLoader.edgeListFile(spark.sparkContext, fileName)

    // Read a file and create an RDD
    val graphTextRDD = spark.sparkContext.textFile(fileName)
    // Create a header that contains the first 4 lines of the text file
    val header = graphTextRDD.take(4)
    // Remove header from the rdd
    val finalGraphTextRDD = graphTextRDD.filter(line => !header.contains(line))

    // Source: https://spark.apache.org/docs/latest/graphx-programming-guide.html
    // Create an RDD of edges
    val edgesRDD: RDD[Edge[Int]] = finalGraphTextRDD.map { line =>
      // Split the line by whitespace and convert the values to integers
      val nodes = line.split("\\s+").map(_.toInt)
      // Create an Edge object with the source and target nodes
      Edge(nodes(0), nodes(1))
    }

    // Create a graph
    val graph: Graph[None.type, Int] = Graph.fromEdges(edgesRDD, None)

    // Question A: Print the 10 vertices with the most incoming edges
    // and the 10 vertices with the most outgoing edges
    // Count in and out edges for each vertex
    val verticesIn = graph.inDegrees
    val verticesOut = graph.outDegrees

    println("10 vertices with the most incoming edges:")
    // Print the top 10 vertices with the highest number of incoming edges, sorted in descending order.
    verticesIn.top(10)(Ordering.by(_._2)).foreach(println)
    println("\n10 vertices with the most outgoing edges:")
    // Print the top 10 vertices with the highest number of outgoing edges, sorted in descending order.
    verticesOut.top(10)(Ordering.by(_._2)).foreach(println)

    // Question B: Calculate the total degree of each vertex and print the count of
    // vertices for which the degree is at least equal to the average total degree

    // Count total degree for each vertex
    val verticesTotalDegrees = graph.degrees
    // Count how many vertices
    val countVertices = verticesTotalDegrees.count().toDouble
    // Count the total sum degree of vertices
    val totalSumVertices = verticesTotalDegrees.values.sum
    // Calculate the average total degree
    val avgTotalDegree = totalSumVertices / countVertices

    // Filter vertices with degree at least equal to the average total degree
    val verticesWithHighDegree = verticesTotalDegrees.filter { case (_, degree) => degree >= avgTotalDegree }

    // Print the count of vertices with high degree
    println("\nCount of vertices with degree at least equal to the average total degree:")
    println(verticesWithHighDegree.count())

    // Stop the spark session
    spark.stop()
 }
}
