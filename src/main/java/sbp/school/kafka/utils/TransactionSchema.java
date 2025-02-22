package sbp.school.kafka.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import sbp.school.kafka.entity.Transaction;

public class TransactionSchema {

    public static final Schema SCHEMA = SchemaBuilder.struct()
            .field("operationType", Schema.STRING_SCHEMA)
            .field("amount", Schema.FLOAT64_SCHEMA)
            .field("account", Schema.STRING_SCHEMA)
            .field("operationDate", Schema.STRING_SCHEMA)
            .build();

    public static Struct getStruct(Transaction transaction) {

        Struct struct = new Struct(SCHEMA);
        struct.put("operation_type", transaction.getOperationType().toString());
        struct.put("amount", transaction.getAmount());
        struct.put("account", transaction.getAccount());
        struct.put("operation_date", transaction.getOperationDate());

        return struct;
    }
}
