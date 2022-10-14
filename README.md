# kafka producer data

通过输入建表语句，创建对应的topic及数据

## 前置条件

### 1. 下载并安装JDK1.8

可参考[Download and Installation Instructions](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)

### 2. 下载安装 Maven

[download maven 3.6.3](https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz)

### 3. 配置参数

#### 3.1 配置kafka-tools.conf

```java
fe_ip=172.31.11.72    //fe ip
http_port=8030        //fe http port
jdbc_port=9030        //fe jdbc port
user=root             // username
password=             // password
database=test_db      // target database name
table=test_tbl        // target table name

rows_per_task=10    //json 数组长度
batch_interval=5000  //多长时间发送一次
        
kafka_bootstrap_servers=127.0.0.1:9092 //kafka 地址
kafka_topic=    //kafka topic 默认db_tbl
```
#### 3.2 配置kafka-tools.sh

```java

CREATE_SQL=`cat << EOF
CREATE TABLE if not exists  test_db.test_tbl (
  id varchar(32),            //varchar类型长度至少32
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
```

### 4. 基本使用

git clone https://github.com/caoliang-web/kafka-tools.git

nohup sh kafka-tools.sh &

### 5. kafka相关命令
```shell
1. 查看条数
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list 127.0.0.1:9092 --topic test_db_test_tbl --time -1

2. 消费数据
./kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --group test --from-beginning --topic test_db_test_tbl

3. 查看消费情况
./kafka-consumer-groups --zookeeper localhost:2181 --describe --group groupName
```

### 5.其他
#### 数据生成规则
字符类型->UUID

布尔类型->随机true/false

数值类型->随机数值

日期类型->当前时间