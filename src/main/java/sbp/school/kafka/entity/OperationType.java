package sbp.school.kafka.entity;

public enum OperationType {

    TRANSFER(0),
    DEPOSIT(1),
    WITHDRAWAL(2);

    private final int code;

    OperationType(int code) {

        this.code = code;
    }

    public int getCode() {

        return code;
    }
}
