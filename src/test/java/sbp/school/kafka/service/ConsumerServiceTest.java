package sbp.school.kafka.service;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.configuration.ConsumerConfiguration;

public class ConsumerServiceTest {

    private ConsumerService consumerService;

    @BeforeMethod
    void init() {

        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration();

        consumerService = new ConsumerService(consumerConfiguration.getProperties());
    }

    @Test
    public void read_test() {

        consumerService.read();
    }
}
