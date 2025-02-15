package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Ack;
import sbp.school.kafka.message.Message;

import java.io.IOException;

import static java.util.Objects.nonNull;

public class AckDeserializer implements Deserializer<Ack> {

    private static final Logger log = LoggerFactory.getLogger(AckDeserializer.class);

    private final ObjectMapper objectMapper;

    public AckDeserializer() {

        objectMapper = new ObjectMapper();
    }

    @Override
    public Ack deserialize(String topic, byte[] bytes) {

        if (nonNull(bytes)) {
            try {

                return objectMapper.readValue(bytes, Ack.class);
            } catch (IOException e) {

                log.error(Message.DESERIALIZATION_ERROR, e.getMessage());

                throw new SerializationException(e);
            }
        } else {
            log.error(Message.DESERIALIZATION_ERROR_BYTES_IS_NULL);

            throw new SerializationException(Message.DESERIALIZATION_ERROR_BYTES_IS_NULL);
        }
    }
}
