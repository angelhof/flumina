package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.PageView;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageViewExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewExperiment.class);

    // More precisely, total PageView events per user per source. Other events are scaled to this.
    private static final int TOTAL_EVENTS = 500_000;

    private final PageViewConfig conf;

    public PageViewExperiment(final PageViewConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final long startTime) throws Exception {
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var getOrUpdateSource =
                new GetOrUpdateSource(TOTAL_EVENTS, conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var getOrUpdateStream = env.addSource(getOrUpdateSource);
        final var pageViewSource =
                new PageViewSource(TOTAL_EVENTS, conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var pageViewStream = env.addSource(pageViewSource)
                .setParallelism(conf.getPageViewParallelism());

        // Broadcast state low-level join
        final var zipCodeDescriptor = new MapStateDescriptor<>("ZipCode",
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
        final var broadcastStream = getOrUpdateStream.keyBy(GetOrUpdate::getUserId).broadcast(zipCodeDescriptor);
        pageViewStream.keyBy(PageView::getUserId)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, PageView, GetOrUpdate, Integer>() {
                    @Override
                    public void processElement(final PageView value, final ReadOnlyContext ctx, final Collector<Integer> out) throws Exception {
                    }

                    @Override
                    public void processBroadcastElement(final GetOrUpdate value, final Context ctx, final Collector<Integer> out) throws Exception {

                    }
                });

        // Normal low-level join
        getOrUpdateStream.keyBy(GetOrUpdate::getUserId)
                .connect(pageViewStream.keyBy(PageView::getUserId));

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
