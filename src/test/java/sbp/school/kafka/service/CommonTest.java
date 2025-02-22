package sbp.school.kafka.service;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.configuration.ConsumerConfiguration;
import sbp.school.kafka.configuration.ProducerConfiguration;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.fail;

public class CommonTest {

    private AckProducerService ackProducerService;

    private TransactionProducerService transactionProducerService;

    private AckConsumerService ackConsumerService;

    private TransactionConsumerService transactionConsumerService;

    private AckService ackService;

    @BeforeMethod
    void init() {

        ProducerConfiguration producerConfiguration = new ProducerConfiguration();

        ackProducerService = new AckProducerService(producerConfiguration.getAckProperties());

        transactionProducerService = new TransactionProducerService(producerConfiguration.getTransactionProperties());

        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration();

        ackConsumerService = new AckConsumerService(consumerConfiguration.getAckProperties(), transactionProducerService);

        transactionConsumerService = new TransactionConsumerService(consumerConfiguration.getTransactionProperties());

        ackService = new AckService(ackProducerService);
    }

    @AfterMethod
    void finish() {

        Optional.ofNullable(ackProducerService)
                .ifPresent(AckProducerService::close);

        Optional.ofNullable(transactionProducerService)
                .ifPresent(TransactionProducerService::close);
    }

    @Test
    public void common_test() throws InterruptedException {

        getTransactions()
                .forEach(transaction -> {
                    try {

                        transactionProducerService.send(transaction);
                    } catch (Exception e) {

                        fail(String.format("Exception: %s", e.getMessage()));
                    }
                });

        ExecutorService pool = Executors.newFixedThreadPool(3);

        pool.submit(() -> transactionConsumerService.read());
        pool.submit(() -> ackService.start());
        pool.submit(() -> ackConsumerService.read());

        Thread.currentThread().join();
    }

    private List<Transaction> getTransactions() {

        LocalDateTime operationDate1 = LocalDateTime.now().minusSeconds(3);
        LocalDateTime operationDate2 = LocalDateTime.now().minusSeconds(2);
        LocalDateTime operationDate3 = LocalDateTime.now().minusSeconds(1);

        return List.of(
                new Transaction(OperationType.TRANSFER, 1000, "1000000001", operationDate1),
                new Transaction(OperationType.DEPOSIT, 2000, "1000000002", operationDate2),
                new Transaction(OperationType.WITHDRAWAL, 3000, "1000000003", operationDate3)
        );
    }
}
