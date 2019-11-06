package edu.upenn.flumina;

import edu.upenn.flumina.data.cases.ValueOrHeartbeat;
import edu.upenn.flumina.source.ValueSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ValueBarrierExperimentTest {

    private static final Logger LOG = LogManager.getLogger();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(4)
                            .build());

    @Test
    public void testSanity() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements(1, 2, 3, 4, 5, 6)
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .reduce((x, y) -> x + y)
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value, Context context) {
                        LOG.info("Cumulative sum: {}", value);
                    }
                }).setParallelism(1);
        env.execute();
    }

    @Test
    public void testValues() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        env.addSource(new ValueSource(10_000, 10.0))
                .timeWindowAll(Time.milliseconds(10_002))
                .aggregate(new AggregateFunction<ValueOrHeartbeat, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(ValueOrHeartbeat valueOrHeartbeat, Integer acc) {
                        return acc + valueOrHeartbeat.match(val -> 1, hb -> 0);
                    }

                    @Override
                    public Integer getResult(Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(Integer acc1, Integer acc2) {
                        return acc1 + acc2;
                    }
                })
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer total, Context context) {
                        LOG.info("Total values: {}", total);
                        assertEquals(total.intValue(), 40_000);
                    }
                }).setParallelism(1);
        env.execute();
    }
}
