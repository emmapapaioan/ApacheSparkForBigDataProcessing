# Big Data Processing using Apache Spark
This repository houses a set of exercises completed as part of a university course on Big Data processing, leveraging Apache Spark, a robust unified analytics engine for large-scale data processing. The exercises encompass various data operations, including DataFrame and RDD transformations, Spark SQL queries, and graph processing.

## Project Overview
- **Exercise 1:** A simple Spark job illustrating how to read data, execute basic transformations, and write data.
- **Exercise 2:** A Spark job that ingests a dataset of airline tweets, analyzing the text of tweets to find the most commonly used words within each sentiment category. It also identifies the primary cause of complaints per airline.
- **Exercise 3:** A Spark job that loads a dataset of movies and carries out operations such as grouping by genre, counting movies per year, and identifying words with specific occurrences in movie titles.
- **Exercise 4:** A Spark job that reads a dataset of web links and performs graph processing to identify vertices with the most incoming and outgoing edges. It also computes the average degree of vertices and counts those with degrees surpassing the average.


## Development Environment
This project was developed using IntelliJ IDEA, a popular Integrated Development Environment (IDE) for Scala and Java development.

The following are the specifics of the development environment:
- **IDE:** IntelliJ IDEA
- **Language Level:** JDK default (8 - Lambdas, type annotations etc.)
- **SDK:** Azul-1.8 (Azul Zulu version 1.8.0_372)

These settings were selected to ensure compatibility and performance. Please ensure your environment meets these requirements if you wish to clone or download and run this project.

Azul Zulu is a certified build of OpenJDK and complies with the Java SE standard for Java 8, 11, and 13. It is the result of rigorous testing and is compliant with all the test suites that Oracle and the OpenJDK community use to test and verify the JDK builds. In this project, we have used Azul Zulu version 1.8.0_372.

If you are using a different setup or newer versions, please make sure to adjust your environment accordingly.

## Running the Project
To execute this project, Spark must be installed and configured on your system. You can then clone this repository and run the code locally.

## Resources
The resources directory of this project contains the datasets (csv and txt files) used for processing tasks in this Apache Spark project. These datasets are essential for executing and validating the code's functionality.

If you wish to use your own data, you can do so by modifying the <strong>fileName</strong> variable in the code to match the relative path of your chosen file.

Due to the large size of these datasets, they may not be directly viewable within the GitHub repository. Although they are included for completeness and to facilitate running the Spark jobs, you may need to download or clone the repository to your local machine to fully access and utilize these datasets.

Remember that when working with big data, the size of datasets can be a challenge in terms of storage and processing, and this project is an example of handling such datasets with Apache Spark.
## Dependencies
- **Apache Spark** 3.1.1
- **Scala** 2.12.13

<em>This repository is publicly available for educational purposes. The exercises present an overview of the capabilities and functionalities of Apache Spark.</em>
