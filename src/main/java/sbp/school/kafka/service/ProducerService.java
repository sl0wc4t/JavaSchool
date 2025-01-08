package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;

import java.util.Optional;
import java.util.Properties;

public class ProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private static final String RESULT_MESSAGE = "{}. offset = {}, partition = {}";

    private final KafkaProducer<String, Transaction> producer;

    private final String topic;

    public ProducerService(Properties properties) {

        producer = new KafkaProducer<>(properties);
        topic = properties.getProperty("topic.name");
    }

    public void send(Transaction transaction) {

        ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, transaction);

        producer.send(record, (recordMetadata, exception) -> Optional.ofNullable(exception)
                .ifPresentOrElse(
                        e -> log.error(RESULT_MESSAGE, e.getMessage(), recordMetadata.offset(), recordMetadata.partition()),
                        () -> log.debug(RESULT_MESSAGE, "Success", recordMetadata.offset(), recordMetadata.partition())
                ));
    }

    public void close() {

        producer.close();
    }
}
