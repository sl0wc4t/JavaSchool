package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;

import java.util.Optional;
import java.util.Properties;

import static sbp.school.kafka.message.Message.RESULT_MESSAGE;

public class TransactionProducerService {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerService.class);

    private final Producer<String, Transaction> producer;

    private final String topic;

    public TransactionProducerService(Properties properties) {

        producer = new KafkaProducer<>(properties);
        topic = properties.getProperty("topic.name");
    }

    public TransactionProducerService(Producer<String, Transaction> producer, String topic) {

        this.producer = producer;
        this.topic = topic;
    }

    public void send(Transaction transaction) {

        TransactionSendInfoService.addSentTransaction(transaction);

        ProducerRecord<String, Transaction> record = new ProducerRecord<>(topic, transaction);

        producer.send(record, (recordMetadata, exception) -> Optional.ofNullable(exception)
                .ifPresentOrElse(
                        e -> handleError(e, recordMetadata),
                        () -> processTransaction(recordMetadata)
                ));
    }

    public void close() {

        producer.close();

        log.info("Transaction Producer is closed");
    }

    private void handleError(Exception e, RecordMetadata recordMetadata) {

        log.error(RESULT_MESSAGE, e.getMessage(), topic, recordMetadata.offset(), recordMetadata.partition());
    }

    private void processTransaction(RecordMetadata recordMetadata) {

        log.info(RESULT_MESSAGE, "Success", topic, recordMetadata.offset(), recordMetadata.partition());
    }
}
