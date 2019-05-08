package utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
public class StreamWriter {
    private KafkaProducer producer;
    public void send(String topic, String key, String value) {
        if (key != null)
            producer.send(new ProducerRecord<String,String>(topic, key, value));
        else
            producer.send(new ProducerRecord<String,String>(topic, value));
    }
    public StreamWriter(String brokers ) {
        Properties properties = new Properties();
        properties.put("acks", "-1");
        properties.put("bootstrap.servers",brokers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(properties);
    }

    public void close() {
        producer.close();
    }
}






