package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.RuleOrHeartbeat;
import edu.upenn.flumina.frauds.data.Transaction;
import edu.upenn.flumina.frauds.data.TransactionOrHeartbeat;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Queue;

public class FraudDetectionManualTest {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionManualTest.class);

    @ClassRule
    public static final MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(3)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testBroadcast() throws Exception {
        // Parameters
        final int totalTrans = 12;
        final double transRate = 0.1;
        final int transNodes = 2;
        final int trRatio = 6;
        final int hbRatio = 2;

        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        final var startTime = Instant.now();
        final var transStream =
                env.addSource(new TransactionOrHeartbeatSource(totalTrans, transRate, startTime)).setParallelism(transNodes);
        final var ruleStream =
                env.addSource(new RuleOrHeartbeatSource(totalTrans, transRate, trRatio, hbRatio, startTime));

        final var broadcastStateDescriptor = new MapStateDescriptor<>("Dummy", Void.class, Void.class);
        final var broadcastRuleStream = ruleStream.broadcast(broadcastStateDescriptor);

        transStream.connect(broadcastRuleStream)
                .process(new BroadcastProcessFunction<TransactionOrHeartbeat, RuleOrHeartbeat, Void>() {
                    private final Queue<Transaction> transactions = new ArrayDeque<>();
                    private final Queue<Rule> rules = new ArrayDeque<>();
                    private final Instant transPhysicalTimestamp = Instant.MIN;
                    private final Instant rulePhysicalTimestamp = Instant.MIN;

                    @Override
                    public void processElement(final TransactionOrHeartbeat transOrHeartbeat,
                                               final ReadOnlyContext ctx,
                                               final Collector<Void> collector) {

                    }

                    @Override
                    public void processBroadcastElement(final RuleOrHeartbeat ruleOrHeartbeat,
                                                        final Context ctx,
                                                        final Collector<Void> collector) {
                    }


                })
                .setParallelism(transNodes);
        env.execute();
    }

}
