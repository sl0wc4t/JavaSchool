package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static sbp.school.kafka.message.Message.PROCESSING_ERROR_MESSAGE;
import static sbp.school.kafka.message.Message.PROCESSING_RESULT_MESSAGE;

public class TransactionConsumerService {
    private static final Logger log = LoggerFactory.getLogger(TransactionConsumerService.class);

    private final Consumer<String, Transaction> consumer;
    private final String topic;

    private List<Transaction> transactions;

    public TransactionConsumerService(Properties properties) {

        consumer = new KafkaConsumer<>(properties);
        topic = properties.getProperty("topic.name");

        transactions = new ArrayList<>();
    }

    public TransactionConsumerService(Consumer<String, Transaction> consumer, String topic, List<Transaction> transactions) {

        this.consumer = consumer;
        this.topic = topic;

        this.transactions = transactions;
    }

    public void read() {

        consumer.subscribe(List.of(topic));

        try {

            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                process(records);

                consumer.commitAsync();
            }
        } catch (WakeupException e) {

            log.error(e.getMessage());
        } catch (Exception e) {

            log.error(PROCESSING_ERROR_MESSAGE, e.getMessage());

            throw new RuntimeException(e);
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
            transactions.add(record.value());

            log.error(PROCESSING_RESULT_MESSAGE,
                    record.topic(),
                    record.offset(),
                    record.partition(),
                    record.value(),
                    consumer.groupMetadata().groupId());

            TransactionSendInfoService.addReceivedTransaction(record.value());
        }
    }

    public void stop() {

        consumer.wakeup();
    }
}
