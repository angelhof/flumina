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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.upenn.flumina.time.TimeHelper.min;

public class FraudProcessFunction extends
        CoProcessFunction<RuleOrHeartbeat, TransactionOrHeartbeat, Tuple3<String, Long, Instant>> {

    private final Tuple2<Long, Long> previousAndCurrentSum = Tuple2.of(0L, 0L);
    private final PriorityQueue<Transaction> transactions = new PriorityQueue<>(new TimestampComparator());
    private final Queue<Rule> rules = new ArrayDeque<>();

    private Instant ruleTimestamp = Instant.MIN;
    private final List<Tuple2<Integer, Instant>> timestampHeap = new ArrayList<>();
    private final List<Integer> timestampPositions = new ArrayList<>();
    private final int transactionParallelism;
    private final int halfTransactionParallelism;

    public FraudProcessFunction(final int transactionParallelism) {
        timestampHeap.addAll(IntStream.range(0, transactionParallelism)
                .mapToObj(i -> Tuple2.of(i, Instant.MIN)).collect(Collectors.toList()));
        timestampPositions.addAll(IntStream.range(0, transactionParallelism)
                .boxed().collect(Collectors.toList()));
        this.transactionParallelism = transactionParallelism;
        halfTransactionParallelism = transactionParallelism >>> 1;
    }

    private void updateTimestamp(final int i, final Instant ts) {
        int tsPos = timestampPositions.get(i);
        final var tup = timestampHeap.get(tsPos);
        tup.f1 = ts;
        while (tsPos < halfTransactionParallelism) {
            int childPos = (tsPos << 1) + 1;
            var child = timestampHeap.get(childPos);
            final int rightChildPos = childPos + 1;
            if (rightChildPos < transactionParallelism
                    && child.f1.compareTo(timestampHeap.get(rightChildPos).f1) > 0) {
                childPos = rightChildPos;
                child = timestampHeap.get(rightChildPos);
            }
            if (ts.compareTo(child.f1) <= 0) {
                break;
            }
            timestampHeap.set(tsPos, child);
            timestampPositions.set(child.f0, tsPos);
            tsPos = childPos;
        }
        timestampHeap.set(tsPos, tup);
        timestampPositions.set(i, tsPos);
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
        updateTimestamp(transactionOrHeartbeat.getSourceIndex(), transactionOrHeartbeat.getPhysicalTimestamp());
        makeProgress(out);
    }

    private Instant getCurrentTimestamp() {
        return min(ruleTimestamp, timestampHeap.get(0).f1);
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