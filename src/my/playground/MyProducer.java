package my.playground;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Created by kzhou on 10/31/16.
 */
public class MyProducer implements Runnable{
    private MyKafkaAccount account;
    private KafkaProducer producer;
    private Properties prop;
    public MyProducer(MyKafkaAccount account) {
        this.account = account;
        prop = new Properties();
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, account.getBootstrapServers());
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put("schema.registry.url", account.getSchemaRegistry());
    }

    public Future<RecordMetadata> produce(String topic, String key, String msg) {
        producer = new KafkaProducer (prop, new StringSerializer(), new StringSerializer());
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    key, msg);
            return producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Future<RecordMetadata> produce(String topic, String key, Map<String, Object> data,
            String schema) {
        Schema.Parser parser = new Schema.Parser();
        Schema actSchema = parser.parse(schema);
        GenericRecord avroRecord = new GenericData.Record(actSchema);
        for(String dataKey: data.keySet()) {
            avroRecord.put(dataKey, data.get(dataKey));
        }
        ProducerRecord record = new ProducerRecord(topic, key, avroRecord);
        Properties newProp = prop;
        newProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        newProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producer = new KafkaProducer(newProp);

        return producer.send(record);


    }

    public void close() {
        if(producer != null) {
            producer.close();
        }
    }

    public void run() {
        try {
            int count = 10;
            String topic = "test";
            String keyPrefix = "key-%d";
            String messageFmt = "{\"type\":\"record\"," +
                    "\"name\":\"myrecord\"," +
                    "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
            while (count > 0) {
                Map<String, Object> data = new HashMap<>();
                data.put("f1", String.format("message%d-%d", count, System.currentTimeMillis()));
                Future<RecordMetadata> future = this.produce(topic, String.format(keyPrefix,
                        count), data, messageFmt);
//                Future<RecordMetadata> future = this.produce(topic, String.format(keyPrefix, count)
//                        , String.format(messageFmt, System.currentTimeMillis()));
                RecordMetadata metadata = future.get();
                System.out.println(String.format("Sent to Topic: %s, Partition: %d, Offset: %d",
                        metadata.topic(), metadata.partition(), metadata.offset()));
                --count;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }
}
