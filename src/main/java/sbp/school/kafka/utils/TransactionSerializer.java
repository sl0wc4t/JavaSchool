package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.nonNull;

public class TransactionSerializer implements Serializer<Transaction> {

    private static final Logger log = LoggerFactory.getLogger(TransactionSerializer.class);

    private static final String ERROR_MESSAGE = "Serialization error: {}";
    private static final String TRANSACTION_IS_NULL_MESSAGE = "Serialization error: Transaction is null";
    private final ObjectMapper objectMapper;

    private final JsonSchemaValidator jsonSchemaValidator;

    public TransactionSerializer() {

        objectMapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .registerModule(new JavaTimeModule());

        jsonSchemaValidator = new JsonSchemaValidator(objectMapper);
    }

    @Override
    public byte[] serialize(String topic, Transaction data) {

        if (nonNull(data)) {
            try {

                String jsonString = objectMapper.writeValueAsString(data);

                jsonSchemaValidator.validate(jsonString);

                return jsonString.getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {

                log.error(ERROR_MESSAGE, e.getMessage());

                throw new SerializationException(e);
            }
        } else {
            log.error(TRANSACTION_IS_NULL_MESSAGE);

            throw new SerializationException(TRANSACTION_IS_NULL_MESSAGE);
        }
    }
}
