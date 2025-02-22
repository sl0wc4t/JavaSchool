package sbp.school.kafka.connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.configuration.DataBaseConfig;
import sbp.school.kafka.entity.OperationType;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.utils.TransactionSchema;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;
import static sbp.school.kafka.message.Message.CONNECTION_CLOSE_ERROR;
import static sbp.school.kafka.message.Message.CONNECTION_ERROR;

public class CustomTransactionSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(CustomTransactionSourceTask.class);

    private static final String SQL_QUERY = """
            select 
              t.* 
            from transactions t 
            where t.offset > ? 
            order by offset 
            limit ? 
            """;
    private static final String TABLE_FIELD = "table";
    private static final String TRANSACTION_TABLE = "transaction";
    private static final String OFFSET = "offset";

    private Connection connection;

    private String topic;
    private int maxBatchSize;

    @Override
    public String version() {

        return new CustomTransactionSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {

        DataBaseConfig dataBaseConfig = new DataBaseConfig(props);

        try {

            connection = DriverManager.getConnection(
                    String.format("%s:%s/%s",
                            props.get(DataBaseConfig.URL_CONFIG),
                            props.get(DataBaseConfig.PORT_CONFIG),
                            props.get(DataBaseConfig.DB_CONFIG)),
                    props.get(DataBaseConfig.USERNAME_CONFIG),
                    props.get(DataBaseConfig.PASSWORD_CONFIG));

            topic = dataBaseConfig.getString(DataBaseConfig.TOPIC_CONFIG);
            maxBatchSize = dataBaseConfig.getInt(DataBaseConfig.MAX_BATCH_SIZE_CONFIG);
        } catch (SQLException e) {

            log.error(CONNECTION_ERROR, e.getMessage());

            throw new RuntimeException(CONNECTION_ERROR, e);
        }
    }

    @Override
    public List<SourceRecord> poll() {

        List<SourceRecord> records = new ArrayList<>();

        try (PreparedStatement ps = connection.prepareStatement(SQL_QUERY)) {

            Map<String, Object> sourcePartition = Collections.singletonMap(TABLE_FIELD, TRANSACTION_TABLE);
            Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);

            long lastOffset = 0L;

            if (nonNull(offset) && nonNull(offset.get(OFFSET))) {
                lastOffset = (Long) offset.get(OFFSET);
            }

            ps.setLong(1, lastOffset);
            ps.setInt(2, maxBatchSize);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Transaction transaction = new Transaction(
                        OperationType.valueOf(rs.getString("operation_type")),
                        rs.getDouble("amount"),
                        rs.getString("account"),
                        LocalDateTime.parse(rs.getString("operation_date"))
                );

                long currentOffset = rs.getLong(OFFSET);

                Map<String, Long> sourceOffset = Collections.singletonMap(OFFSET, currentOffset);

                records.add(new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        TransactionSchema.SCHEMA,
                        TransactionSchema.getStruct(transaction)
                ));
            }
        } catch (SQLException e) {
            log.error(CONNECTION_ERROR, e.getMessage());

            throw new RuntimeException(CONNECTION_ERROR, e);
        }

        return records;
    }

    @Override
    public void stop() {

        try {

            if (nonNull(connection)) {
                connection.close();
            }
        } catch (SQLException e) {

            log.error(CONNECTION_CLOSE_ERROR, e);

            throw new RuntimeException(CONNECTION_CLOSE_ERROR, e);
        }
    }
}
