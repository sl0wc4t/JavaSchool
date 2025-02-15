package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Ack;

import java.util.Optional;
import java.util.Properties;

import static sbp.school.kafka.message.Message.RESULT_MESSAGE;

public class AckProducerService {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerService.class);

    private final KafkaProducer<String, Ack> producer;

    private final String topic;

    public AckProducerService(Properties properties) {

        producer = new KafkaProducer<>(properties);
        topic = properties.getProperty("topic.name");
    }

    public void send(Ack ack) {

        ProducerRecord<String, Ack> record = new ProducerRecord<>(topic, ack);

        producer.send(record, (recordMetadata, exception) -> Optional.ofNullable(exception)
                .ifPresentOrElse(
                        e -> log.error(RESULT_MESSAGE, e.getMessage(), topic, recordMetadata.offset(), recordMetadata.partition()),
                        () -> log.error(RESULT_MESSAGE, "Success", topic, recordMetadata.offset(), recordMetadata.partition())
                ));
    }

    public void close() {

        producer.close();
    }
}
