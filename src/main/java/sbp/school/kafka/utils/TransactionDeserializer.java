package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.message.Message;

import java.io.IOException;

import static java.util.Objects.nonNull;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private static final Logger log = LoggerFactory.getLogger(TransactionDeserializer.class);

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

                log.error(Message.DESERIALIZATION_ERROR, e.getMessage());

                throw new SerializationException(e);
            }
        } else {
            log.error(Message.DESERIALIZATION_ERROR_BYTES_IS_NULL);

            throw new SerializationException(Message.DESERIALIZATION_ERROR_BYTES_IS_NULL);
        }
    }
}
