package sbp.school.kafka.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConsumerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfiguration.class);

    private static final String TRANSACTION_CONSUMER_PROPERTIES_FILE_PATH = "src/main/resources/transaction-consumer.properties";

    private static final String ACK_CONSUMER_PROPERTIES_FILE_PATH = "src/main/resources/ack-consumer.properties";

    private final Properties transactionProperties = new Properties();

    private final Properties ackProperties = new Properties();

    public ConsumerConfiguration() {

        try {

            transactionProperties.load(new FileInputStream(TRANSACTION_CONSUMER_PROPERTIES_FILE_PATH));
            ackProperties.load(new FileInputStream(ACK_CONSUMER_PROPERTIES_FILE_PATH));
        } catch (IOException e) {

            log.error("Loading consumer configuration file failed: {}", e.getMessage());

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
