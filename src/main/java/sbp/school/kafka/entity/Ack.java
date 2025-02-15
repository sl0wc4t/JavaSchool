package sbp.school.kafka.entity;

public class Ack {

    private Long since;

    private Long until;

    private int hash;

    public Ack() {

    }

    public Ack(Long since, Long until, int hash) {

        this.since = since;
        this.until = until;
        this.hash = hash;
    }

    public Long getSince() {

        return since;
    }

    public Long getUntil() {

        return until;
    }

    public int getHash() {

        return hash;
    }
}
