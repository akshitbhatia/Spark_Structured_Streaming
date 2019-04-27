# Spark_Structured_Streaming
Spark structured streaming using socket, Making dataframe eg:(Name, Age, Gender, Company, Year) using Window operations and watermark, Data written to specified file sink.

## To run the project:
* Install jdk 8 on your linux system(Ubuntu).
* Install Intellij on Ubuntu and add project in it.
* Edit file Spark_Structured_Streaming.scala ,write file sink to your desire location.
* Open terminal for start net cat utility and hit this command : nc -l 9999 
* Run the project on your Intellij
* Start giving input on your netcat terminal semicolumn seperated and space seperated for spliting: for example: akshit;25;Male;plp;2017
