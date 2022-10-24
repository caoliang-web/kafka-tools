package com.flywheels.doris.connector;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerThread implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerThread.class);


    private KafkaProducer<String, String> producer = null;

    private ProducerRecord<String, String> record = null;


    public KafkaProducerThread(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        this.producer = producer;
        this.record = record;
    }


    @Override
    public void run() {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(null != e){
                    LOG.error("send message exception:{}",e.getMessage());
                }
                if(null != recordMetadata){
                    LOG.info(String.format("offset:%s,partition:%s", recordMetadata.offset(), recordMetadata.partition()));
                }
            }
        });
    }
}
