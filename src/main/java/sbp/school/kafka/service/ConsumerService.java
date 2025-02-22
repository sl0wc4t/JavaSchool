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

public class ConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerService.class);

    private static final String PROCESSING_RESULT_MESSAGE = "topic = {}, partition = {}, offset = {}, value = {}, groupId = {}";
    private static final String PROCESSING_ERROR_MESSAGE = "Processing record failed {}";

    private final Consumer<String, Transaction> consumer;
    private final String topic;

    private List<Transaction> transactions;

    public ConsumerService(Properties properties) {

        consumer = new KafkaConsumer<>(properties);
        topic = properties.getProperty("topic.name");

        transactions = new ArrayList<>();
    }

    public ConsumerService(Consumer<String, Transaction> consumer, String topic, List<Transaction> transactions) {

        this.consumer = consumer;
        this.topic = topic;

        this.transactions = transactions;
    }

    public void read() {

        consumer.subscribe(List.of(topic));

        try {

            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Transaction> record : records) {
                    transactions.add(record.value());

                    log.debug(PROCESSING_RESULT_MESSAGE,
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.value(),
                            consumer.groupMetadata().groupId());
                }

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

    public void stop() {

        consumer.wakeup();
    }
}
