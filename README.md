# Big Data Processing using Apache Spark
This repository houses a set of exercises completed as part of a university course on Big Data processing, leveraging Apache Spark, a robust unified analytics engine for large-scale data processing. The exercises encompass various data operations, including DataFrame and RDD transformations, Spark SQL queries, and graph processing.

## Project Overview
<ul>
  <li><strong>Exercise 1:</strong> A simple Spark job illustrating how to read data, execute basic transformations, and write data.</li> 

  <li><strong>Exercise 2:</strong> A Spark job that ingests a dataset of airline tweets, analyzing the text of tweets to find the most commonly used words within each sentiment category. It also identifies the primary cause   of complaints per airline.</li> 

  <li><strong>Exercise 3:</strong> A Spark job that loads a dataset of movies and carries out operations such as grouping by genre, counting movies per year, and identifying words with specific occurrences in movie titles.   </li> 

  <li><strong>Exercise 4:</strong> A Spark job that reads a dataset of web links and performs graph processing to identify vertices with the most incoming and outgoing edges. It also computes the average degree of vertices    and counts those with degrees surpassing the average.</li> 
</ul>

## Running the Project
To execute this project, Spark must be installed and configured on your system. You can then clone this repository and run the code locally.

## Resources
The resources directory of this project contains the datasets (csv and txt files) used for processing tasks in this Apache Spark project. These datasets are essential for executing and validating the code's functionality.

If you wish to use your own data, you can do so by modifying the <strong>fileName</strong> variable in the code to match the relative path of your chosen file.

Due to the large size of these datasets, they may not be directly viewable within the GitHub repository. Although they are included for completeness and to facilitate running the Spark jobs, you may need to download or clone the repository to your local machine to fully access and utilize these datasets.

Remember that when working with big data, the size of datasets can be a challenge in terms of storage and processing, and this project is an example of handling such datasets with Apache Spark.
## Dependencies
Apache Spark
Scala

<em>This repository is publicly available for educational purposes. The exercises present an overview of the capabilities and functionalities of Apache Spark.</em>
