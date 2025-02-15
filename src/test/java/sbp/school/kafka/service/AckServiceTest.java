package sbp.school.kafka.service;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sbp.school.kafka.configuration.ProducerConfiguration;

public class AckServiceTest {

    private AckService ackService;

    @BeforeMethod
    void init() {

        ProducerConfiguration producerConfiguration = new ProducerConfiguration();

        AckProducerService ackProducerService = new AckProducerService(producerConfiguration.getAckProperties());

        ackService = new AckService(ackProducerService);
    }

    @Test
    public void read_test() {

        ackService.start();
    }
}
