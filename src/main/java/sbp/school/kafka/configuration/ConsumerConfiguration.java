package sbp.school.kafka.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConsumerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfiguration.class);

    private static final String CONSUMER_PROPERTIES_FILE_PATH = "src/main/resources/consumer.properties";

    private final Properties properties = new Properties();

    public ConsumerConfiguration() {

        try {

            properties.load(new FileInputStream(CONSUMER_PROPERTIES_FILE_PATH));
        } catch (IOException e) {

            log.error("Loading consumer configuration file failed: {}", e.getMessage());

            throw new RuntimeException(e);
        }
    }

    public Properties getProperties() {

        return properties;
    }
}
