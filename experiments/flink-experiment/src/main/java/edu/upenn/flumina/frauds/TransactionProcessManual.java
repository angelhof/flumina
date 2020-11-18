package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.RuleOrHeartbeat;
import edu.upenn.flumina.frauds.data.Transaction;
import edu.upenn.flumina.frauds.data.TransactionOrHeartbeat;
import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static edu.upenn.flumina.time.TimeHelper.min;

public class TransactionProcessManual extends BroadcastProcessFunction<TransactionOrHeartbeat, RuleOrHeartbeat, Tuple3<String, Long, Instant>> {

    private Instant transactionPhysicalTimestamp = Instant.MIN;
    private Instant rulePhysicalTimestamp = Instant.MIN;
    private Tuple2<Long, Long> previousAndCurrentSum = Tuple2.of(0L, 0L);

    private final Queue<Transaction> transactions = new ArrayDeque<>();
    private final Queue<Rule> rules = new ArrayDeque<>();

    private final String rmiHost;
    private final String fraudDetectionServiceName;
    private transient ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>> fraudDetectionService;

    public TransactionProcessManual(final String rmiHost, final String fraudDetectionServiceName) {
        this.rmiHost = rmiHost;
        this.fraudDetectionServiceName = fraudDetectionServiceName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(final Configuration parameters) throws RemoteException, NotBoundException {
        final var registry = LocateRegistry.getRegistry(rmiHost);
        fraudDetectionService =
                (ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>>) registry.lookup(fraudDetectionServiceName);
    }

    @Override
    public void processElement(final TransactionOrHeartbeat transactionOrHeartbeat,
                               final ReadOnlyContext ctx,
                               final Collector<Tuple3<String, Long, Instant>> out) throws RemoteException {
        transactions.addAll(transactionOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        transactionPhysicalTimestamp = transactionOrHeartbeat.getPhysicalTimestamp();
        makeProgress(out);
    }

    @Override
    public void processBroadcastElement(final RuleOrHeartbeat ruleOrHeartbeat,
                                        final Context ctx,
                                        final Collector<Tuple3<String, Long, Instant>> out) throws RemoteException {
        rules.addAll(ruleOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        rulePhysicalTimestamp = ruleOrHeartbeat.getPhysicalTimestamp();
        makeProgress(out);
    }

    private void makeProgress(final Collector<Tuple3<String, Long, Instant>> out) throws RemoteException {
        final var currentTime = min(transactionPhysicalTimestamp, rulePhysicalTimestamp);
        while (!transactions.isEmpty() &&
                transactions.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
            final var transaction = transactions.remove();
            while (!rules.isEmpty() &&
                    rules.element().getPhysicalTimestamp().isBefore(transaction.getPhysicalTimestamp())) {
                join(rules.remove());
            }
            update(transaction, out);
        }
        while (!rules.isEmpty() &&
                rules.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
            join(rules.remove());
        }
    }

    private void update(final Transaction transaction, final Collector<Tuple3<String, Long, Instant>> out) {
        if (previousAndCurrentSum.f0 % 1000L == transaction.val % 1000L) {
            out.collect(Tuple3.of("Transaction", transaction.val, transaction.getPhysicalTimestamp()));
        }
        previousAndCurrentSum.f1 += transaction.val;
    }

    private void join(final Rule rule) throws RemoteException {
        previousAndCurrentSum =
                fraudDetectionService.joinChild(getRuntimeContext().getIndexOfThisSubtask(), previousAndCurrentSum);
    }

}
