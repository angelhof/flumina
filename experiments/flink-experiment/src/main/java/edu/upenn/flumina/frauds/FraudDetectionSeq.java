package edu.upenn.flumina.frauds;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.FraudDetectionConfig;
import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.Transaction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static edu.upenn.flumina.time.TimeHelper.toEpochMilli;

public class FraudDetectionSeq implements Experiment {

    private final FraudDetectionConfig conf;

    public FraudDetectionSeq(final FraudDetectionConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var transactionSource = new TransactionSource(conf.getTotalValues(), conf.getValueRate(), startTime);
        final var transactionStream = env.addSource(transactionSource)
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("transactions");
        final var ruleSource = new RuleSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(),
                conf.getHeartbeatRatio(), startTime);
        final var ruleStream = env.addSource(ruleSource)
                .slotSharingGroup("rules");

        final var transactionsDescriptor = new ValueStateDescriptor<>("UnprocessedTransactions",
                TypeInformation.of(new TypeHint<PriorityQueue<Transaction>>() {
                }));
        final var rulesDescriptor = new ValueStateDescriptor<>("UnprocessedRules",
                TypeInformation.of(new TypeHint<Deque<Rule>>() {
                }));
        final var sumDescriptor = new ValueStateDescriptor<>("Sum",
                TypeInformation.of(Long.class));
        final var previousSumDescriptor = new ValueStateDescriptor<>("PreviousSum",
                TypeInformation.of(Long.class));

        ruleStream.keyBy(x -> 0)
                .connect(transactionStream.keyBy(x -> 0))
                .process(new KeyedCoProcessFunction<Integer, Rule, Transaction, Tuple3<String, Long, Instant>>() {

                    private ValueState<Long> sumState;
                    private ValueState<Long> previousSumState;
                    private ValueState<PriorityQueue<Transaction>> transactionsState;
                    private ValueState<Deque<Rule>> rulesState;

                    @Override
                    public void open(final Configuration parameters) {
                        sumState = getRuntimeContext().getState(sumDescriptor);
                        previousSumState = getRuntimeContext().getState(previousSumDescriptor);
                        transactionsState = getRuntimeContext().getState(transactionsDescriptor);
                        rulesState = getRuntimeContext().getState(rulesDescriptor);
                    }

                    @Override
                    public void processElement1(final Rule rule,
                                                final Context ctx,
                                                final Collector<Tuple3<String, Long, Instant>> out) throws Exception {
                        initRules();
                        rulesState.value().addAll(rule.match(List::of, hb -> Collections.emptyList()));
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                    }

                    @Override
                    public void processElement2(final Transaction transaction,
                                                final Context ctx,
                                                final Collector<Tuple3<String, Long, Instant>> out) throws Exception {
                        initTransactions();
                        transactionsState.value().addAll(transaction.match(List::of, hb -> Collections.emptyList()));
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                    }

                    @Override
                    public void onTimer(final long timestamp,
                                        final OnTimerContext ctx,
                                        final Collector<Tuple3<String, Long, Instant>> out) throws Exception {
                        initRules();
                        final var rules = rulesState.value();
                        initTransactions();
                        final var transactions = transactionsState.value();

                        while (!rules.isEmpty() &&
                                toEpochMilli(rules.getFirst().getPhysicalTimestamp()) <= timestamp) {
                            final var rule = rules.removeFirst();
                            while (!transactions.isEmpty() &&
                                    transactions.element().getPhysicalTimestamp()
                                            .isBefore(rule.getPhysicalTimestamp())) {
                                update(transactions.remove(), out);
                            }
                            update(rule, out);
                        }
                        while (!transactions.isEmpty() &&
                                toEpochMilli(transactions.element().getPhysicalTimestamp()) <= timestamp) {
                            update(transactions.remove(), out);
                        }
                    }

                    private void initRules() throws IOException {
                        if (rulesState.value() == null) {
                            rulesState.update(new ArrayDeque<>());
                        }
                    }

                    private void initTransactions() throws IOException {
                        if (transactionsState.value() == null) {
                            transactionsState.update(new PriorityQueue<>(
                                    Comparator.comparing(Transaction::getPhysicalTimestamp)));
                        }
                    }

                    private void update(final Rule rule,
                                        final Collector<Tuple3<String, Long, Instant>> out) throws IOException {
                        if (sumState.value() == null) {
                            sumState.update(0L);
                        }
                        out.collect(Tuple3.of("Rule", sumState.value(), rule.getPhysicalTimestamp()));
                        previousSumState.update(sumState.value());
                        sumState.update(0L);
                    }

                    private void update(final Transaction transaction,
                                        final Collector<Tuple3<String, Long, Instant>> out) throws IOException {
                        if (sumState.value() == null) {
                            sumState.update(0L);
                        }
                        if (previousSumState.value() == null) {
                            previousSumState.update(0L);
                        }
                        if (previousSumState.value() % 100L == transaction.getVal() % 100L) {
                            out.collect(Tuple3.of("Transaction", transaction.getVal(), transaction.getPhysicalTimestamp()));
                        }
                        sumState.update(sumState.value() + transaction.getVal());
                    }
                })
                .slotSharingGroup("rules")
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

        return env.execute("FraudDetection Experiment");
    }

    @Override
    public long getTotalEvents() {
        return conf.getValueNodes() * conf.getTotalValues() + conf.getTotalValues() / conf.getValueBarrierRatio();
    }

    @Override
    public long getOptimalThroughput() {
        return (long) (conf.getValueRate() * conf.getValueNodes() + conf.getValueRate() / conf.getValueBarrierRatio());
    }

}
