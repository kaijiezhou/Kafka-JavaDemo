package my.playground;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by kzhou on 11/1/16.
 */
public class MyConsumer implements Runnable{
    private KafkaConsumer<String, String> consumer;
    private MyKafkaAccount account;
    private Properties prop;
    enum DataType {
        STRING,
        AVRO
    }
    public MyConsumer(MyKafkaAccount account, String groupID, DataType type) {
        this.account = account;
        prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, account.getBootstrapServers());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        prop.put("schema.registry.url", account.getSchemaRegistry());
        switch (type) {
            case STRING:
                prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer
                        .class);
                break;
            case AVRO:
                prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
                prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer
                        .class);
                break;
            default:
                prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
                prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        }
        consumer = new KafkaConsumer(prop);
    }

    public void consume(String topic, int msgCount) {
        consumer.subscribe(Arrays.asList(topic));
        while (msgCount > 0) {
            ConsumerRecords records = consumer.poll(Long.MAX_VALUE);
            Iterator<ConsumerRecord> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                ConsumerRecord record = recordIterator.next();
                process(record);
                TopicPartition topicPartition = new TopicPartition(record.topic(), record
                        .partition());
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset());
                Map<TopicPartition, OffsetAndMetadata> commitTarget = new HashMap<>();
                commitTarget.put(topicPartition, offsetAndMetadata);
                consumer.commitAsync(commitTarget, new ConsumerCoordinator.DefaultOffsetCommitCallback());
                --msgCount;
                if(msgCount==0) {
                    break;
                }
            }
        }
        consumer.unsubscribe();
    }

    public void consume(String topic) {
        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords records = consumer.poll(Long.MAX_VALUE);
                Iterator<ConsumerRecord> recordIterator = records.iterator();
                while (recordIterator.hasNext()) {
                    ConsumerRecord record = recordIterator.next();
                    process(record);
                }
                consumer.commitAsync();
            }
        } finally {
            if(consumer != null) {
                consumer.commitAsync();
                consumer.close();
            }
        }
    }

    public void consume (String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        long curOffset = consumer.position(topicPartition);
        consumer.seek(topicPartition, offset);
        ConsumerRecords records = consumer.poll(Long.MAX_VALUE);
        Iterator<ConsumerRecord> recordIterator = records.iterator();
        if (recordIterator.hasNext()) {
            process(recordIterator.next());
        } else {
            System.err.println("Unavailable Offset!");
        }
        consumer.seek(topicPartition, curOffset);
        consumer.unsubscribe();
    }

    private void resetOffset(String topic, int partition, long offset) {
        if (consumer == null) return;
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.seek(topicPartition, offset);

    }

    private void process(ConsumerRecord record) {
        System.out.println(String.format("Message Received: %s, %s", record.key(), record.value()));
        System.out.println(String.format("[Metadata: From topic %s, partition: " +
                "%d, offset: %d. Timestamp: %d, Receive timestamp: %d]", record.topic(), record
                .partition(), record.offset(), record.timestamp(), System.currentTimeMillis()));
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }


    public void run() {
        String topic = "test";
        this.consume(topic, 0, 0);
        this.consume(topic, 0, 1);
        this.consume(topic, 10);
    }
}
