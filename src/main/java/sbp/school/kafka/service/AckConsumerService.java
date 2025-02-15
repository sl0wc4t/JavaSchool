package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Ack;
import sbp.school.kafka.entity.Transaction;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static sbp.school.kafka.message.Message.PROCESSING_ERROR_MESSAGE;
import static sbp.school.kafka.message.Message.PROCESSING_RESULT_MESSAGE;

public class AckConsumerService {

    private static final Logger log = LoggerFactory.getLogger(AckConsumerService.class);

    private final KafkaConsumer<String, Ack> consumer;

    private final TransactionProducerService producerService;

    public AckConsumerService(Properties properties, TransactionProducerService producerService) {

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(properties.getProperty("topic.name")));

        this.producerService = producerService;
    }

    public void read() {

        try {

            while (true) {
                ConsumerRecords<String, Ack> records = consumer.poll(Duration.ofMillis(100));

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

    private void process(ConsumerRecords<String, Ack> records) {

        for (ConsumerRecord<String, Ack> record : records) {
            log.error(PROCESSING_RESULT_MESSAGE,
                    record.topic(),
                    record.offset(),
                    record.partition(),
                    record.value(),
                    consumer.groupMetadata().groupId());

            Ack ack = record.value();

            List<Transaction> sentTransactions = TransactionSendInfoService.getSentTransactionsByPeriod(ack.getSince(), ack.getUntil());

            int expectedHash = Long.hashCode(sentTransactions.stream()
                    .mapToLong(Transaction::getId)
                    .sum());

            if (ack.getHash() == expectedHash) {
                TransactionSendInfoService.removeTransactions(sentTransactions);
            } else {
                log.error("Transactions with ids in {} will be resent",
                        sentTransactions.stream()
                                .map(Transaction::getId)
                                .toList());

                sentTransactions
                        .forEach(producerService::send);
            }
        }
    }
}
