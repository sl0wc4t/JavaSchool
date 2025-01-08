package sbp.school.kafka.service;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.configuration.ProducerConfiguration;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.fail;

public class ProducerServiceTest {

    private ProducerService producerService;

    @BeforeMethod
    void init() {

        ProducerConfiguration producerConfiguration = new ProducerConfiguration();

        producerService = new ProducerService(producerConfiguration.getProperties());
    }

    @AfterMethod
    void finish() {

        Optional.ofNullable(producerService)
                .ifPresent(ProducerService::close);
    }

    @Test
    public void send_test() {

        getTransactions()
                .forEach(transaction -> {
                    try {

                        producerService.send(transaction);
                    } catch (Exception e) {

                        fail(String.format("Exception: %s", e.getMessage()));
                    }
                });
    }

    List<Transaction> getTransactions() {

        return List.of(
                new Transaction(OperationType.TRANSFER, 1000, "1000000001", LocalDateTime.now()),
                new Transaction(OperationType.DEPOSIT, 2000, "1000000002", LocalDateTime.now()),
                new Transaction(OperationType.WITHDRAWAL, 3000, "1000000003", LocalDateTime.now())
        );
    }
}
