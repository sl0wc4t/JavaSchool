package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;

public class JsonSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(JsonSchemaValidator.class);

    private static final String JSON_SCHEMA_FILE_PATH = "src/main/resources/transaction-schema.json";

    private final ObjectMapper objectMapper;

    private final JsonSchema schema;

    public JsonSchemaValidator(ObjectMapper objectMapper) {

        this.objectMapper = objectMapper;
        this.schema = getSchema();
    }

    private JsonSchema getSchema() {

        try {

            JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

            return jsonSchemaFactory.getSchema(new FileInputStream(JSON_SCHEMA_FILE_PATH));
        } catch (IOException e) {

            log.error("Loading json-schema file failed: {}", e.getMessage());

            throw new RuntimeException(e);
        }
    }

    public void validate(String jsonString) throws JsonProcessingException {

        JsonNode jsonNode = objectMapper.readTree(jsonString);

        Set<ValidationMessage> validationResults = schema.validate(jsonNode);

        if (!validationResults.isEmpty()) {

            log.error("Validation error: {}", validationResults);

            throw new RuntimeException(validationResults.toString());
        }
    }
}
