#!/usr/bin/env bash

set -e -x

\cd "$(dirname "$0")/../jars"
echo "Downloading JAR files to $(\pwd)"

# For deployment
# NB If you make changes here, also update upload_env.sh which "knows" these versions (and also update the README)
wget -v https://jdbc.postgresql.org/download/postgresql-9.4.1208.jar
wget -v https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.2.1.1001.jar

# For local testing ONLY, versions match EMR 5.0.0
# wget -v http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar
# wget -v http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4.2/aws-java-sdk-1.7.4.2.jar

# For local testing ONLY, versions match EMR 5.3.0 (2016.09)
wget -v http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar

# aws-java-sdk fails with an error when using matching or latest version:
#   java.lang.NoSuchMethodError: com.amazonaws.services.s3.transfer.TransferManager
wget -v http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4.2/aws-java-sdk-1.7.4.2.jar
