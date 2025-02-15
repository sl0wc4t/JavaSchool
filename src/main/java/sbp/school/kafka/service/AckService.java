package sbp.school.kafka.service;

import sbp.school.kafka.entity.Ack;
import sbp.school.kafka.entity.Transaction;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class AckService {

    private final AckProducerService producerService;

    public AckService(AckProducerService producerService) {

        this.producerService = producerService;
    }

    public void start() {

        while (true) {
            try {

                Thread.sleep(35000);
            } catch (InterruptedException e) {

                throw new RuntimeException(e);
            }

            Long since = Timestamp.valueOf(LocalDateTime.now().minusSeconds(65)).getTime();
            Long until = Timestamp.valueOf(LocalDateTime.now().minusSeconds(5)).getTime();

            int hash = Long.hashCode(
                    TransactionSendInfoService.getReceivedTransactionsByPeriod(since, until).stream()
                            .mapToLong(Transaction::getId)
                            .sum()
            );

            producerService.send(new Ack(since, until, hash));
        }
    }
}
