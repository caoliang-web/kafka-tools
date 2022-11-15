package com.flywheels.doris.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flywheels.doris.util.JdbcUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.ThreadPool;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

import static com.flywheels.doris.util.Constants.*;

public class KafkaPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class);
    private static JSONObject jsonObject;
    private static Random random = new Random();
    private static String[] COLUMNS_KEYS;
    private static String topic;
    private static Map<String, String> colType = new HashMap<>();


    public static void main(String[] args) throws Exception {

        /*if (args.length < 1) {
            LOG.error("缺少配置参数");
            System.exit(1);
        }*/
        String properString = IOUtils.toString(new FileInputStream("/Users/caoliang/Documents/kafka/kafka-tools/kafka-tools.conf"), "UTF-8");

        LOG.info("doris相关参数{}", properString);
        jsonObject = JSON.parseObject(properString);

        JSONArray task = jsonObject.getJSONArray("task");

        ExecutorService executorService = Executors.newFixedThreadPool(task.size());

        try {
            for (int i = 0; i < task.size(); i++) {
                final int index = i;
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        JSONObject jsonObject1 = task.getJSONObject(index);
                        System.out.println(jsonObject1.toJSONString());
                        executorTask(jsonObject1);
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("Send message exception:{}", e.getMessage());
        } finally {
            executorService.shutdown();
        }
        Thread.sleep(1000);
    }

    public static void initSchema(String schemaSQL) {
        if (StringUtils.isBlank(schemaSQL)) {
            return;
        }
        String url = jsonObject.getString(KEY_FE_IP) + ":" + jsonObject.getString(KEY_JDBC_PORT);
        String user = jsonObject.getString(KEY_USER);
        String password = jsonObject.getString(KEY_PASSWORD);
        try {
            JdbcUtil.executeBatch(url, "", user, password, schemaSQL.trim().split(";"));
        } catch (SQLException e) {
            LOG.error("create schema sql execute fail:", e);
            throw new RuntimeException(e);
        }

    }

    public static void mockJson(JSONObject jsonStr) {
        List<String> columnsKeys = new ArrayList<>();
        String url = jsonObject.getString(KEY_FE_IP) + ":" + jsonObject.getString(KEY_JDBC_PORT);
        String db = jsonStr.getString(KEY_DATABASE);
        String tbl = jsonStr.getString(KEY_TABLE);
        String user = jsonObject.getString(KEY_USER);
        String password = jsonObject.getString(KEY_PASSWORD);
        String sql = "desc " + db + "." + tbl;
        List<JSONObject> result = JdbcUtil.executeQuery(url, db, user, password, sql);
        for (JSONObject jsob : result) {
            String name = jsob.getString("Field");
            String type = removeParentheses(jsob.getString("Type"));
            String key = jsob.getString("Key");
            if (StringUtils.isNotEmpty(key) && "true".equals(key)) {
                if (type.toUpperCase().equals("INT") || type.toUpperCase().equals("BIGINT")) {
                    columnsKeys.add(name);
                }
            }
            colType.put(jsob.getString("Field"), removeParentheses(jsob.getString("Type")));
        }
        COLUMNS_KEYS = columnsKeys.toArray(new String[]{});

    }


    public static JSONObject makeJson() {
        JSONObject jsonObject = new JSONObject();
        colType.forEach((k, v) -> {
            Object value = null;
            switch (v.toUpperCase(Locale.ROOT)) {
                case "BOOLEAN":
                    value = random.nextBoolean();
                    break;
                case "FLOAT":
                    value = random.nextFloat();
                    break;
                case "DOUBLE":
                    value = random.nextDouble();
                    break;
                case "TINYINT":
                    value = (byte) random.nextInt(100);
                    break;
                case "SMALLINT":
                    value = random.nextInt(30000);
                    break;
                case "INTEGER":
                    value = random.nextInt(2147483647);
                    break;
                case "INT":
                    value = random.nextInt(2147483647);
                    break;
                case "BIGINT":
                    value = random.nextLong();
                    break;
                case "DECIMAL":
                    value = random.nextFloat();
                    break;
                case "DATE":
                    value = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    break;
                case "DATETIME":
                    value = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    break;
                case "CHAR":
                    value = UUID.randomUUID().toString().substring(0, 1);
                    break;
                case "VARCHAR":
                    value = UUID.randomUUID().toString().replaceAll("-", "");
                    break;
                default:
                    LOG.warn("can not find column {} data type:{}", k, v);
            }
            jsonObject.put(k, value);
        });

        return jsonObject;
    }


    public static Map<String, String> load(String propertiesString) throws IOException {

        Properties properties = new Properties();
        properties.load(new StringReader(propertiesString));
        return new HashMap<>((Map) properties);
    }

    private static String removeParentheses(String str) {
        int head = str.indexOf("(");
        if (head == -1) {
            return str;
        } else {
            int next = head + 1;
            int count = 1;
            while (head != -1) {
                if (str.charAt(next) == '(') {
                    count++;
                } else if (str.charAt(next) == ')') {
                    count--;
                }
                next++;
                if (count == 0) {
                    String temp = str.substring(head, next);
                    str = str.replace(temp, "");
                    head = str.indexOf('(');
                    next = head + 1;
                    count = 1;
                }
            }
        }
        return str;
    }

    public static void executorTask(JSONObject json) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", jsonObject.getString(KAFKA_BOOTSTRAP_SERVERS));
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        mockJson(json);
        initSchema(json.getString("sql"));
        float repeatRate = 0;
        int keyRange = 0;
        long batchInterval = 10;
        int batchSize = Integer.valueOf(json.getString(KEY_ROWS_PER_TASK));
        if (StringUtils.isNotBlank(json.getString(KEY_REPEAT_RATE))) {
            try {
                repeatRate = Float.valueOf(json.getString(KEY_REPEAT_RATE));
            } catch (Exception ex) {
                LOG.warn("repeatRate parse error,use default value 0");
            }
        }
        if (StringUtils.isNotBlank(json.getString(KEY_KEY_RANGE))) {
            try {
                keyRange = Integer.valueOf(json.getString(KEY_KEY_RANGE));
            } catch (Exception ex) {
                LOG.warn("keyRange parse error,use default value 0");
            }
        }
        if (StringUtils.isNotBlank(json.getString(KEY_BATCH_INTERVAL))) {
            try {
                batchInterval = Long.parseLong(json.getString(KEY_BATCH_INTERVAL));
            } catch (Exception ex) {
                LOG.warn("batchInterval parse error,use default value 1000");
            }
        }
        if (StringUtils.isBlank(json.getString(KAFKA_TOPIC))) {
            topic = json.getString(KEY_DATABASE) + "_" + json.getString(KEY_TABLE);
        } else {
            topic = json.getString(KAFKA_TOPIC);
        }

        int totalNum;
        if (StringUtils.isBlank(json.getString(KEY_TOTAL_NUMBER))) {
            totalNum = Integer.MAX_VALUE;
        } else {
            totalNum = Integer.valueOf(json.getString(KEY_TOTAL_NUMBER));

        }
        ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(json.getString(KEY_THREAD_NUM)));
        ProducerRecord<String, String> record;
        Boolean bool = true;
        int count = 0;
        while (bool) {
            for (int j = 0; j < batchSize; j++) {
                JSONArray jsonArray = new JSONArray();
                //random row
                JSONObject json1 = makeJson();
                //重复率参数
                if (repeatRate > 0 && j < batchSize * repeatRate && COLUMNS_KEYS.length > 0) {
                    json1.put(COLUMNS_KEYS[0], random.nextInt(batchSize));
                }

                //将key的基数扩大
                if (keyRange > 0) {
                    for (int keyIndex = 0; keyIndex < COLUMNS_KEYS.length; keyIndex++) {
                        json1.put(COLUMNS_KEYS[keyIndex], random.nextInt(keyRange));
                    }
                }
                jsonArray.add(json1);
                record = new ProducerRecord<String, String>(topic, jsonArray.toJSONString());
                executor.submit(new KafkaProducerThread(producer, record));
            }
            count = count + batchSize;
            LOG.warn("线程-:{} , topic : {} , 导入行数：{}", Thread.currentThread().getName(), topic, count);
            if (count >= totalNum) {
                bool = false;
            }
            try {
                Thread.sleep(batchInterval);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        executor.shutdown();
        producer.close();
    }
}
