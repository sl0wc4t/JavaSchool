package sbp.school.kafka.message;

public class Message {

    public static final String SERIALIZATION_ERROR = "Serialization error: {}";
    public static final String SERIALIZATION_ERROR_DATA_IS_NULL = "Serialization error: Data is null";

    public static final String DESERIALIZATION_ERROR = "Deserialization error: {}";
    public static final String DESERIALIZATION_ERROR_BYTES_IS_NULL = "Deserialization error: Bytes is null";

    public static final String RESULT_MESSAGE = "{}. topic = {}, offset = {}, partition = {}";

    public static final String PROCESSING_RESULT_MESSAGE = "topic = {}, offset = {}, partition = {}, value = {}, groupId = {}";
    public static final String PROCESSING_ERROR_MESSAGE = "Processing record failed {}";
}
