package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static sbp.school.kafka.message.Message.PROCESSING_ERROR_MESSAGE;
import static sbp.school.kafka.message.Message.PROCESSING_RESULT_MESSAGE;

public class TransactionConsumerService {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumerService.class);

    private final KafkaConsumer<String, Transaction> consumer;

    public TransactionConsumerService(Properties properties) {

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(properties.getProperty("topic.name")));
    }

    public void read() {

        try {

            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                process(records);

                consumer.commitAsync();
            }
        } catch (Exception e) {

            log.error(PROCESSING_ERROR_MESSAGE, e.getMessage());
        } finally {

            try {

                consumer.commitSync();
            } finally {

                consumer.close();
            }
        }
    }

    private void process(ConsumerRecords<String, Transaction> records) {

        for (ConsumerRecord<String, Transaction> record : records) {
            log.error(PROCESSING_RESULT_MESSAGE,
                    record.topic(),
                    record.offset(),
                    record.partition(),
                    record.value(),
                    consumer.groupMetadata().groupId());

            TransactionSendInfoService.addReceivedTransaction(record.value());
        }
    }
}
