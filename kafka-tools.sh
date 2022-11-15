#!/usr/bin/env bash

basepath=$(cd `dirname $0`; pwd)

#编译
cd $basepath
mvn clean package
if [ $? -ne 0 ]; then echo "compile source fail."; exit 1; fi

#提交jar
java -jar $basepath/target/kafka-tools.jar  $basepath/kafka-tools.conf