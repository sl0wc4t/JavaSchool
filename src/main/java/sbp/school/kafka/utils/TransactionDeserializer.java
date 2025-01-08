package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;

import java.io.IOException;

import static java.util.Objects.nonNull;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private static final Logger log = LoggerFactory.getLogger(TransactionDeserializer.class);

    private static final String ERROR_MESSAGE = "Deserialization error: {}";
    private static final String BYTES_IS_NULL_MESSAGE = "Deserialization error: Bytes is null";
    private final ObjectMapper objectMapper;

    private final JsonSchemaValidator jsonSchemaValidator;

    public TransactionDeserializer() {

        objectMapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .registerModule(new JavaTimeModule());

        jsonSchemaValidator = new JsonSchemaValidator(objectMapper);
    }

    @Override
    public Transaction deserialize(String s, byte[] bytes) {

        if (nonNull(bytes)) {
            try {

                Transaction transaction = objectMapper.readValue(bytes, Transaction.class);

                jsonSchemaValidator.validate(transaction);

                return transaction;
            } catch (IOException e) {

                log.error(ERROR_MESSAGE, e.getMessage());

                throw new SerializationException(e);
            }
        } else {
            log.error(BYTES_IS_NULL_MESSAGE);

            throw new SerializationException(BYTES_IS_NULL_MESSAGE);
        }
    }
}
