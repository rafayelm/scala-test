# scala-test
data manipulation and aggregations with scala

### How to run
Install Java and Maven

Do mvn clean package

Create a folder and put the clicks and impressions files in the folder.
Run the jar file named scala-test-1.0-SNAPSHOT-jar-with-dependencies.jar
with 3 arguments.
The arguments are: 
1. path to the folder, containing clicks and impressions
2. clicks file name
3. impressions file name

Here is example of how to run

`java -jar <path to jar> <path to folder> clicks.json impressions.json`

The output of the program will be in the folder, the path of which was provided in first arg.
The program outputs 2 json files named `metrics.json` and `recommendations.json`
