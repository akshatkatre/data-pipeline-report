# Report Generation with Spark

The project will outline the website traffic json report generation with Spark.

The skeleton code was given in (https://github.com/jlcanela/spark-hands-on/wiki). 

The code has been modified to do the following:

- Read a Apache web server log file
- Identify all the high traffic dates where the webserver has more than 20000 requests
- For each high traffic date 
	- compute the list of number of access by URI for each URI
	- compute the list of number of access per IP address for each IP addres
	- compute the list of number of connections from 10  days before to 10 days after
- Generate the report in a JSON file format

## Prerequisites
- Java 8
- Scala 2.11
- SBT
- Spark-2.4.5 with Hadoop 2.7
- IntelliJ IDE (Optional)

### CreateReport.scala
The com.dsti.report.CreateReport.scala file encapsulates the logic for generating the web traffic json report.
- The entry point into CreateReport.scala will be the method generateReport. The program will do the following:
	- Read the apache web server log file.
	- Clean, convert and enrich the contents of the log file to a spark data frame
	- Use spark.sql to identify the dates where the number of requests exceeded 20,000 records
	- For each of the dates invoke returnReportRow function
		- Get count by IP for the date and store in data frame
		- Get count by URI for the date and store in data frame
		- Get count of traffic 10 days prior and 10 days after and store in data frame
	- Combine the 3 data frames into a single data frame.
	- Store the the resultant data frame in an Array
	- Combine all the array results into a single data frame
	- Write the results of the data frame as a json output

#### Unit Tests
- The unit test cases are encapsulated in the src/test/scala/UnitTest.scala file
- The unit test cases follow the the structure of Given-When-Then format
- To enable the unit test cases sample data has been created based on the apache web server log format. 
- The sample data is present in src/test/resources/sample.log
- There are 3 unit tests that are executed, the details have been documented below:

##### Scenario 1 - Assert  _SUCCESS file
The test case will clean the sample report output directory and then invoke the method createReport, that will in-turn invoke generate report of com.dsti.report.CreateReport object with the sample log file as input


The following assertion will be done - Validate if the _SUCCESS file has been created in the report export directory

##### Scenario 2 - Assert JSON file count
The following assertion will be done - Validate if the count of JSON files in the report export directory is equal to 2.

##### Assert JSON content count
The following assertion will be done - Read the JSON files in the report directory and assert the following (for each file):
 - Validate the column names that are generated are as per expectation
 - Validate if the count for each report is greater than 20,000
 - Validate if the date column as a date that is in the expected date list


## Usage
Download/Clone the repository

### Build the fatjar
Access the repository home and run the following command from the prompt

sbt assembly

The fatjar will be located in target/scala-2.11. Take note of the absolute path of the jar file 

### Generate Report 
The report will be generate via the Spark submit utility. 
To generate the report navigate to the bin directory of spark home

On windows

.\spark-submit --class com.dsti.report.CreateReport --master "local[*]"  <absolute_path_of_fatjar> "absolute_path_of_log_file" "absolute_path_of_output_directory" 


### Known Issues
- When the program is run in spark-shell it throws an Out of Memory exception and terminates.

