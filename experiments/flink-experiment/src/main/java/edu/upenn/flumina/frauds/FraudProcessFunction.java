package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.RuleOrHeartbeat;
import edu.upenn.flumina.frauds.data.Transaction;
import edu.upenn.flumina.frauds.data.TransactionOrHeartbeat;
import edu.upenn.flumina.util.TimestampComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.*;

import static edu.upenn.flumina.time.TimeHelper.min;

public class FraudProcessFunction extends
        CoProcessFunction<RuleOrHeartbeat, TransactionOrHeartbeat, Tuple3<String, Long, Instant>> {

    private final List<Instant> transactionTimestamps = new ArrayList<>();
    private final Tuple2<Long, Long> previousAndCurrentSum = Tuple2.of(0L, 0L);
    private final PriorityQueue<Transaction> transactions = new PriorityQueue<>(new TimestampComparator());
    private final Queue<Rule> rules = new ArrayDeque<>();
    private Instant ruleTimestamp = Instant.MIN;

    public FraudProcessFunction(final int transactionParallelism) {
        transactionTimestamps.addAll(Collections.nCopies(transactionParallelism, Instant.MIN));
    }

    @Override
    public void processElement1(final RuleOrHeartbeat ruleOrHeartbeat,
                                final Context ctx,
                                final Collector<Tuple3<String, Long, Instant>> out) {
        rules.addAll(ruleOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        ruleTimestamp = ruleOrHeartbeat.getPhysicalTimestamp();
        makeProgress(out);
    }

    @Override
    public void processElement2(final TransactionOrHeartbeat transactionOrHeartbeat,
                                final Context ctx,
                                final Collector<Tuple3<String, Long, Instant>> out) {
        transactions.addAll(transactionOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        transactionTimestamps.set(transactionOrHeartbeat.getSourceIndex(),
                transactionOrHeartbeat.getPhysicalTimestamp());
        makeProgress(out);
    }

    private Instant getCurrentTimestamp() {
        final var transactionTimestamp = transactionTimestamps.stream().min(Instant::compareTo).get();
        return min(transactionTimestamp, ruleTimestamp);
    }

    private void makeProgress(final Collector<Tuple3<String, Long, Instant>> out) {
        final var currentTimestamp = getCurrentTimestamp();
        while (!rules.isEmpty() &&
                rules.element().getPhysicalTimestamp().compareTo(currentTimestamp) <= 0) {
            final var rule = rules.remove();
            while (!transactions.isEmpty() &&
                    transactions.element().getPhysicalTimestamp().isBefore(rule.getPhysicalTimestamp())) {
                update(transactions.remove(), out);
            }
            update(rule, out);
        }
        while (!transactions.isEmpty() &&
                transactions.element().getPhysicalTimestamp().compareTo(currentTimestamp) <= 0) {
            update(transactions.remove(), out);
        }
    }

    private void update(final Rule rule,
                        final Collector<Tuple3<String, Long, Instant>> out) {
        out.collect(Tuple3.of("Rule", previousAndCurrentSum.f1, rule.getPhysicalTimestamp()));
        previousAndCurrentSum.f0 = previousAndCurrentSum.f1;
        previousAndCurrentSum.f1 = 0L;
    }

    private void update(final Transaction transaction,
                        final Collector<Tuple3<String, Long, Instant>> out) {
        if (previousAndCurrentSum.f0 % 1000L == transaction.val % 1000L) {
            out.collect(Tuple3.of("Transaction", transaction.val, transaction.getPhysicalTimestamp()));
        }
        previousAndCurrentSum.f1 += transaction.val;
    }

}