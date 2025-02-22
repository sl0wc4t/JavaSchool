package sbp.school.kafka.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ProducerConfiguration.class);

    private static final String TRANSACTION_PRODUCER_PROPERTIES_FILE_PATH = "src/main/resources/transaction-producer.properties";
    private static final String ACK_PRODUCER_PROPERTIES_FILE_PATH = "src/main/resources/ack-producer.properties";

    private final Properties transactionProperties = new Properties();
    private final Properties ackProperties = new Properties();

    public ProducerConfiguration() {

        try {

            transactionProperties.load(new FileInputStream(TRANSACTION_PRODUCER_PROPERTIES_FILE_PATH));
            ackProperties.load(new FileInputStream(ACK_PRODUCER_PROPERTIES_FILE_PATH));
        } catch (IOException e) {

            log.error("Loading producer configuration file failed: {}", e.getMessage());

            throw new RuntimeException(e);
        }
    }

    public Properties getTransactionProperties() {

        return transactionProperties;
    }

    public Properties getAckProperties() {

        return ackProperties;
    }
}
