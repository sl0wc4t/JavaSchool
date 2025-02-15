package sbp.school.kafka.service;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.configuration.ConsumerConfiguration;

public class TransactionConsumerServiceTest {

    private TransactionConsumerService transactionConsumerService;

    @BeforeMethod
    void init() {

        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration();

        transactionConsumerService = new TransactionConsumerService(consumerConfiguration.getTransactionProperties());
    }

    @Test
    public void read_test() {

        transactionConsumerService.read();
    }
}
