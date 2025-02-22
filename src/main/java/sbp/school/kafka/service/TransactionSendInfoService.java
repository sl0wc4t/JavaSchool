package sbp.school.kafka.service;

import sbp.school.kafka.entity.Transaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionSendInfoService {

    private static final Map<Long, Transaction> sentTransactions = new ConcurrentHashMap<>();
    private static final Map<Long, Transaction> receivedTransactions = new ConcurrentHashMap<>();

    public static void addSentTransaction(Transaction transaction) {

        sentTransactions.put(transaction.getId(), transaction);
    }

    public static void addReceivedTransaction(Transaction transaction) {

        receivedTransactions.put(transaction.getId(), transaction);
    }

    public static void removeTransactions(List<Transaction> transactions) {

        transactions.stream()
                .mapToLong(Transaction::getId)
                .forEach(transactionId -> {
                    sentTransactions.remove(transactionId);
                    receivedTransactions.remove(transactionId);
                });
    }

    public static List<Transaction> getSentTransactionsByPeriod(Long since, Long until) {

        return getTransactionsByPeriod(since, until, sentTransactions);
    }

    public static List<Transaction> getReceivedTransactionsByPeriod(Long since, Long until) {

        return getTransactionsByPeriod(since, until, receivedTransactions);
    }

    private static List<Transaction> getTransactionsByPeriod(Long since, Long until, Map<Long, Transaction> transactions) {

        return transactions.entrySet().stream()
                .filter(entry -> entry.getKey() >= since && entry.getKey() < until)
                .map(Map.Entry::getValue)
                .toList();
    }
}
