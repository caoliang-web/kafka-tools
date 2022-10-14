#!/usr/bin/env bash

basepath=$(cd `dirname $0`; pwd)

#参数配置
CREATE_SQL=`cat << EOF
create database if not exists test_db;
drop table if exists test_db.test_tbl;
CREATE TABLE test_db.test_tbl (
  id varchar(256),
  c_boolean boolean,
  c_char char(1),
  c_date date,
  c_datetime datetime,
  c_decimal decimal(10,2),
  c_double double,
  c_float float,
  c_int int,
  c_bigint bigint,
  c_largeint largeint,
  c_smallint smallint,
  c_string string,
  c_tinyint tinyint
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
'replication_allocation' = 'tag.location.default: 1'
)
EOF`

echo $CREATE_SQL

#编译
cd $basepath
mvn clean package
if [ $? -ne 0 ]; then echo "compile source fail."; exit 1; fi

#提交jar
java -jar $basepath/target/kafka-tools.jar  $basepath/flink-tools.conf "$CREATE_SQL"