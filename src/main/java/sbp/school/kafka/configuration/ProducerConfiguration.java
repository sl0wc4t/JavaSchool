package sbp.school.kafka.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ProducerConfiguration.class);

    private static final String PRODUCER_PROPERTIES_FILE_PATH = "src/main/resources/producer.properties";

    private final Properties properties = new Properties();

    public ProducerConfiguration() {

        try {

            properties.load(new FileInputStream(PRODUCER_PROPERTIES_FILE_PATH));
        } catch (IOException e) {

            log.error("Loading producer configuration file failed: {}", e.getMessage());

            throw new RuntimeException(e);
        }
    }

    public Properties getProperties() {

        return properties;
    }
}
