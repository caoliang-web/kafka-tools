package com.flywheels.doris.connector;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDO {
    //初始化logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class);
    private Properties props;


    public KafkaProducerDO() {
    }

    /**
     * 获得props
     *
     * @return the props
     */
    public Properties getProps() {
        return props;
    }

    /**
     * 设置props
     *
     * @param props the props to set
     */
    public void setProps(Properties props) {
        this.props = props;
    }

    /**
     * 发布
     */
    public void publish(String topic, String message) {
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
            producer.send(record);
        } catch (Exception e) {
            LOG.error("publish error: " + e);
            throw new RuntimeException("publish topic = " + topic + " error: " + e);
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
