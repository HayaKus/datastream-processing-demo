#!/bin/sh

echo "build module: flink-connector-pulsar"
cd ../flink-connector-pulsar
mvn clean install -X -Dcheckstyle.skip

echo "build module: flink-connector-zhiyan"
cd ../flink-connector-zhiyan
mvn clean install -X -Dcheckstyle.skip

#echo "build module: flink-connector-clickhouse"
#cd ../flink-connector-clickhouse
#mvn clean install -X -Dcheckstyle.skip

echo "build module: flink-connector-ctsdb"
cd ../flink-connector-ctsdb
mvn clean install -X -Dcheckstyle.skip

echo "build module: datastream-processing-main"
cd ../datastream-processing-main
mvn clean package
