# Report Generation with Spark

The project will outline the website traffic report generation with Spark.
The skeleton code was given in (https://github.com/jlcanela/spark-hands-on/wiki). The code has been modified to do the following:

- Read a Apache web server log file
- Identify all the high traffic dates where the webserver has more than 20000 requests
- For each high traffic date 
	- compute the list of number of access by URI for each URI
	- compute the list of number of access per IP address for each IP addres
	- compute the list of number of connections from 10  days before to 10 days after
- Generate the report in a JSON file format


The lab has been implemented in GenerateReport.sc file (for spark-shell)
The lab has been implemented in CreateReport.scala file (for spark-submit)

## Prerequsites
- IntelliJ IDE
- Spark-2.4.5 with Hadoop 2.7
- Download the file the repository: https://github.com/akshatkatre/spark-datapipeline/blob/master/GenerateReport.sc
- Access the scala shell. 

## Spark Shell Usage
- Copy the contents of GenerateReport.sc and paste them into the Scala Shell
- To execute the report generate logic invoke the function generateReport from scala-shell. The function takes 2 parameters, the first parameter take a String with the absolute path of the log file. The second parameter will take the report directory path, the report directory should not already exist. 
- The below three lines of show how the generateReport should be invoked. Set the inputLogFile and reportDirectory values based on your environment setup.


val inputLogFile = "/user/home/spark-hands-on-master/webserver-log/access.log"

val reportDirectory = "/user/home/webserver-report"

generateReport(inputLogFile, reportDirectory)


### GenerateReport.sc
- The entry point into GenerateReport.sc will be the method generateReport. The program will do the following:
	- Read the log file.
	- Clean, convert and enrich the contents of the log file to dataframe
	- Use spark.sql to identify the dates where the number of requests exceeded 20,000 hits
	- For each of the dates invoke returnReportRow function
		- Get count by IP for the date and store in data frame
		- Get count by URI for the date and store in data frame
		- Get count of traffic 10 days prior and 10 days after and store in data frame
		- Combine the 3 data frames into a single data frame. Return the data frame
	- Store the results of returnReportRow method in an Array
	- Combine all the array results into a single data frame
	- Write the results of the data frame as a json output
	- Run 3 assertions to validate the output

#### Unit Tests
##### Assert Success message
The following assertion will be done - Validate if the _SUCCESS file has been created in the report export directory

##### Assert JSON file count
The following assertion will be done - Validate if the count of JSON files in the report export directory is the same as the number of records with over 20000 hits

##### Assert JSON content count
The following assertion will be done - Pick the first JSON file to identify the date and website hit count. For that specific date compare the website hit count with the count for that date in the spark data frame.

### report.CreateReport.scala
A new scala object CreateReport has been created to enable the generation of the JAR file. The code is a near replica of GenerateReport.sc, there are some additions around creating the spark context. The main method of the CreateReport object takes two arguments:

- The first argument is the path to the log file that needs to be read and processed.
- The second argument is the path to the report directory.

## Spark Submit Usage (Production)
To execute the report in production we will need to use spark-submit application. The spark-submit application needs a JAR file to be created.

The below steps are specific to IntelliJ IDE.
- Clone the project and use the source code in an IntelliJ Scala - Sbt project
- The report generation code is encapulated in the CreateReport.scala file (https://github.com/akshatkatre/spark-datapipeline/blob/master/src/main/scala/reports/CreateReport.scala)
- The build.sbt is important as the version of spark and scala need to be correct for spark-submit application to work
- Open the command prompt and navigate to the IntelliJ Project home. When you navigate to this level you should be able to see src, target folders and build.sbt file when to list command
- To build the JAR file run the command:

sbt package

- The JAR file will be located in the target folder. Take note of absolute path of the JAR file
- Navigate to bin directory of spark installation home. 
- Below is how the spark-submit needs to be executed on windows. The spark-submit application takes multiple arguments, the class name needs to be passed, followed by the path of the JAR file, followed by the arguments to the class report.CreateReport. The first argument is the location of the log file that needs to be read and processed, the second argument is the folder location to where the report output needs to be written. Please note - it this directory already exists the spark application will error and the report won't be generated.

./spark-submit --class report.CreateReport 
<Absolute_path_to_JAR_file> <Absolute_path_to_log_file> <Abosolute_path_to_report_directory>

### Known Issues
- When the program is run in spark-shell it throws an Out of Memory exception and terminates. To get around the problem a limit has been set on SQL statements to only get a specific number of URIs and iP addresses. The limit is set in the variable sqlLimit. The program currently has the limit set to 100. The SQL statements already have an order by clause, which will get the highest count records first.

