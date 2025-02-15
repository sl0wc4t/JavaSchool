package sbp.school.kafka.service;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.configuration.ConsumerConfiguration;
import sbp.school.kafka.configuration.ProducerConfiguration;

import java.util.Optional;

public class AckConsumerServiceTest {

    private TransactionProducerService transactionProducerService;

    private AckConsumerService ackConsumerService;

    @BeforeMethod
    void init() {
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();

        transactionProducerService = new TransactionProducerService(producerConfiguration.getTransactionProperties());

        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration();

        ackConsumerService = new AckConsumerService(consumerConfiguration.getAckProperties(), transactionProducerService);
    }

    @AfterMethod
    void finish() {

        Optional.ofNullable(transactionProducerService)
                .ifPresent(TransactionProducerService::close);
    }

    @Test
    public void read_test() {

        ackConsumerService.read();
    }
}
