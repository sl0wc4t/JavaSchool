package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;

import java.time.LocalDateTime;
import java.util.*;

import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.*;

public class TransactionConsumerServiceTest {

    private static final String TOPIC_NAME = "transaction-events-test";
    private static final Integer PARTITION = 0;

    private MockConsumer<String, Transaction> mockConsumer;

    private List<Transaction> transactions;

    private TransactionConsumerService transactionConsumerService;

    @BeforeMethod
    void init() {

        openMocks(this);

        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.subscribe(List.of(TOPIC_NAME));

        transactions = new ArrayList<>();

        transactionConsumerService = new TransactionConsumerService(mockConsumer, TOPIC_NAME, transactions);
    }

    @AfterMethod
    void finish() {

        Optional.ofNullable(mockConsumer)
                .ifPresent(MockConsumer::close);
    }

    @Test(description = "Успешная обработка")
    void read_test_01() {

        Transaction transaction =
                new Transaction(OperationType.TRANSFER, 1000, "1000000001", LocalDateTime.now());

        List<Transaction> expectedTransactions = List.of(transaction);

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(List.of(new TopicPartition(TOPIC_NAME, PARTITION)));
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC_NAME, 0, 0, null, transaction));
        });
        mockConsumer.schedulePollTask(() -> transactionConsumerService.stop());
        mockConsumer.updateBeginningOffsets(Map.of(new TopicPartition(TOPIC_NAME, PARTITION), 0L));


        transactionConsumerService.read();


        assertEquals(expectedTransactions.size(), transactions.size());
        assertTrue(expectedTransactions.containsAll(transactions));
        assertTrue(transactions.containsAll(expectedTransactions));
    }

    @Test(description = "Ошибка при обработке")
    void read_test_02() {

        mockConsumer.schedulePollTask(() -> mockConsumer.setPollException(new KafkaException("Kafka Exception")));
        mockConsumer.schedulePollTask(() -> transactionConsumerService.stop());

        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC_NAME, PARTITION);
        startingOffsets.put(tp, 0L);
        mockConsumer.updateBeginningOffsets(startingOffsets);


        RuntimeException exception = null;

        try {

            transactionConsumerService.read();
        } catch (RuntimeException e) {

            exception = e;
        }


        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Kafka Exception"));
    }
}
