package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageViewExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewExperiment.class);

    private static final int TOTAL_USERS = 2;
    private static final int TOTAL_EVENTS = 1_000_000;

    private final PageViewConfig conf;

    public PageViewExperiment(final PageViewConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final long startTime) throws Exception {
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final GetOrUpdateSource getOrUpdateSource =
                new GetOrUpdateSource(TOTAL_EVENTS, TOTAL_USERS, 10.0, startTime);
        final DataStream<GetOrUpdateOrHeartbeat> getOrUpdateStream = env.addSource(getOrUpdateSource);
        final PageViewSource pageViewSource =
                new PageViewSource(TOTAL_EVENTS, TOTAL_USERS, 100.0, startTime);
        final DataStream<PageViewOrHeartbeat> pageViewStream = env.addSource(pageViewSource).setParallelism(10);

        // TODO

        return env.execute("PageView Experiment");
    }

    @Override
    public long getTotalEvents() {
        // TODO
        return 0;
    }

    @Override
    public long getOptimalThroughput() {
        // TODO
        return 0;
    }

}
