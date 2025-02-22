package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.utils.TransactionSerializer;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.*;

public class ProducerServiceTest {

    private static final String TOPIC_NAME = "transaction-events-test";

    private MockProducer<String, Transaction> mockProducer;

    private ProducerService producerService;

    @BeforeMethod
    void init() {

        mockProducer = new MockProducer<>(true, new StringSerializer(), new TransactionSerializer());

        producerService = new ProducerService(mockProducer, TOPIC_NAME);
    }

    @AfterMethod
    void finish() {

        Optional.ofNullable(producerService)
                .ifPresent(ProducerService::close);
    }

    @Test(description = "Успешная отправка")
    public void send_test_01() {

        List<Transaction> expectedTransactions = getTransactions();


        expectedTransactions
                .forEach(producerService::send);


        List<Transaction> actualTransactions = mockProducer.history().stream()
                .map(ProducerRecord::value)
                .toList();

        assertEquals(expectedTransactions.size(), actualTransactions.size());
        assertTrue(expectedTransactions.containsAll(actualTransactions));
        assertTrue(actualTransactions.containsAll(expectedTransactions));
    }

    @Test(description = "Ошибка валидации")
    public void send_test_02() {

        Transaction transaction = new Transaction(OperationType.TRANSFER, 1000, "1", LocalDateTime.now());

        RuntimeException exception = null;


        try {

            producerService.send(transaction);
        } catch (RuntimeException e) {

            exception = e;
        }


        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("account: длина должна быть не менее 10 символов"));
    }

    private List<Transaction> getTransactions() {

        return List.of(
                new Transaction(OperationType.TRANSFER, 1000, "1000000001", LocalDateTime.now()),
                new Transaction(OperationType.DEPOSIT, 2000, "1000000002", LocalDateTime.now()),
                new Transaction(OperationType.WITHDRAWAL, 3000, "1000000003", LocalDateTime.now())
        );
    }
}
