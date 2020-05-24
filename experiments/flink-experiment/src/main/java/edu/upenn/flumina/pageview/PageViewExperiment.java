package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.Update;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
        // env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var getOrUpdateSource =
                new GetOrUpdateSource(TOTAL_EVENTS, conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var getOrUpdateStream = env.addSource(getOrUpdateSource);
        final var pageViewSource =
                new PageViewSource(TOTAL_EVENTS, conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var pageViewStream = env.addSource(pageViewSource)
                .setParallelism(conf.getPageViewParallelism());

        // Normal low-level join
        getOrUpdateStream.keyBy(GetOrUpdate::getUserId)
                .connect(pageViewStream.keyBy(PageView::getUserId))
                .process(new KeyedCoProcessFunction<Integer, GetOrUpdate, PageView, Update>() {

                    private final ValueStateDescriptor<Integer> zipCodeDescriptor = new ValueStateDescriptor<>(
                            "ZipCode",
                            TypeInformation.of(Integer.class)
                    );

                    private ValueState<Integer> zipCodeState;

                    @Override
                    public void open(final Configuration parameters) {
                        zipCodeState = getRuntimeContext().getState(zipCodeDescriptor);
                    }

                    @Override
                    public void processElement1(final GetOrUpdate getOrUpdate,
                                                final Context ctx,
                                                final Collector<Update> out) throws IOException {
                        if (zipCodeState.value() == null) {
                            // Store some initial value; could be more sophisticated
                            zipCodeState.update(10_000);
                        }
                        getOrUpdate.match(
                                get -> null,
                                update -> {
                                    try {
                                        zipCodeState.update(update.getZipCode());
                                    } catch (final IOException e) {
                                        // Ignore
                                    }
                                    out.collect(update);
                                    return null;
                                }
                        );
                    }

                    @Override
                    public void processElement2(final PageView pageView,
                                                final Context ctx,
                                                final Collector<Update> out) {

                    }
                })
                .setParallelism(conf.getTotalUsers())
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        return env.execute("PageView Experiment");
    }

    @Override
    public long getTotalEvents() {
        // PageView events + Get events + Update events
        return (TOTAL_EVENTS * conf.getPageViewParallelism() + TOTAL_EVENTS / 100 + TOTAL_EVENTS / 1000) *
                conf.getTotalUsers();
    }

    @Override
    public long getOptimalThroughput() {
        return (long) ((conf.getPageViewParallelism() + 0.011) * conf.getTotalUsers() * conf.getPageViewRate());
    }

}
