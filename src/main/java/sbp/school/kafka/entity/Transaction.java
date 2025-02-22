package sbp.school.kafka.entity;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class Transaction {

    private Long id;

    private OperationType operationType;

    private double amount;

    private String account;

    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            timezone = "UTC"
    )
    private LocalDateTime operationDate;

    public Transaction() {

    }

    public Transaction(OperationType operationType, double amount, String account, LocalDateTime operationDate) {

        this.id = Timestamp.valueOf(operationDate).getTime();
        this.operationType = operationType;
        this.amount = amount;
        this.account = account;
        this.operationDate = operationDate;
    }

    public Long getId() {

        return id;
    }

    public OperationType getOperationType() {

        return operationType;
    }

    public double getAmount() {

        return amount;
    }

    public String getAccount() {

        return account;
    }

    public LocalDateTime getOperationDate() {

        return operationDate;
    }

    public void setId(Long id) {

        this.id = id;
    }
}
