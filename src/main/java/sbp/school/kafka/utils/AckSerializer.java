package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Ack;
import sbp.school.kafka.message.Message;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.nonNull;

public class AckSerializer implements Serializer<Ack> {

    private static final Logger log = LoggerFactory.getLogger(AckSerializer.class);

    private final ObjectMapper objectMapper;

    public AckSerializer() {

        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Ack data) {

        if (nonNull(data)) {
            try {

                String value = objectMapper.writeValueAsString(data);

                return value.getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {

                log.error(Message.SERIALIZATION_ERROR, e.getMessage());

                throw new SerializationException(e);
            }
        } else {
            log.error(Message.SERIALIZATION_ERROR_DATA_IS_NULL);

            throw new SerializationException(Message.SERIALIZATION_ERROR_DATA_IS_NULL);
        }
    }
}
