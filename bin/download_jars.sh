#!/usr/bin/env bash

# Download all the jar files needed to run Spark jobs

set -e -x

\cd `dirname $0`/../jars
echo "Downloading JAR files to `\pwd`"

# For deployment
wget -v https://jdbc.postgresql.org/download/postgresql-9.4.1208.jar
wget -v http://repo1.maven.org/maven2/com/databricks/spark-csv_2.10/1.4.0/spark-csv_2.10-1.4.0.jar
wget -v http://central.maven.org/maven2/org/apache/commons/commons-csv/1.4/commons-csv-1.4.jar
wget -v https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.10.1010.jar

# For local testing
wget -v http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar
wget -v http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4.2/aws-java-sdk-1.7.4.2.jar
