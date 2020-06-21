package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class ProducerSyncConnectorCheck {

    private static final String UNIQUE_ID = UUID.randomUUID().toString();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UNIQUE_ID);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.initTransactions();

        try {
            kafkaProducer.beginTransaction();
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("datasync_topic",
                    "{\"id\":\"secret ID\"}",
                    "{\n" +
                            "\t\"type\": \"oci/site\",\n" +
                            "\t\"name\": \"WEE\",\n" +
                            "\t\"_type\": \"BTS\",\n" +
                            "\t\"status\": \"Live\"\n" +
                            "}");
            kafkaProducer.send(producerRecord);
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
            e.printStackTrace();
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
