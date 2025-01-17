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

public class ConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerService.class);

    private static final String PROCESSING_RESULT_MESSAGE = "topic = {}, partition = {}, offset = {}, value = {}, groupId = {}";
    private static final String PROCESSING_ERROR_MESSAGE = "Processing record failed {}";

    private final KafkaConsumer<String, Transaction> consumer;

    public ConsumerService(Properties properties) {

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(properties.getProperty("topic.name")));
    }

    public void read() {

        try {

            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Transaction> record : records) {
                    log.debug(PROCESSING_RESULT_MESSAGE,
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.value(),
                            consumer.groupMetadata().groupId());
                }

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
}
