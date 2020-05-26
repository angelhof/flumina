package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.data.TimestampedCore;
import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.Update;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
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
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.PriorityQueue;

import static edu.upenn.flumina.time.TimeHelper.toEpochMilli;

public class PageViewExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewExperiment.class);

    private final PageViewConfig conf;

    public PageViewExperiment(final PageViewConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        // env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var getOrUpdateSource = new GetOrUpdateSource(conf.getTotalPageViews(), conf.getTotalUsers(),
                conf.getPageViewRate(), startTime);
        final var getOrUpdateStream = env.addSource(getOrUpdateSource);
        final var pageViewSource =
                new PageViewSource(conf.getTotalPageViews(), conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var pageViewStream = env.addSource(pageViewSource)
                .setParallelism(conf.getPageViewParallelism());

        final var zipCodeDescriptor = new ValueStateDescriptor<>("ZipCode", TypeInformation.of(Integer.class));
        final var updateBufferDescriptor = new ValueStateDescriptor<>("UpdateBuffer",
                TypeInformation.of(new TypeHint<Deque<Update>>() {
                }));
        final var pageViewBufferDescriptor = new ValueStateDescriptor<>("PageViewBuffer",
                TypeInformation.of(new TypeHint<PriorityQueue<PageView>>() {
                }));

        // Normal low-level join
        getOrUpdateStream.keyBy(GetOrUpdate::getUserId)
                .connect(pageViewStream.keyBy(PageView::getUserId))
                .process(new KeyedCoProcessFunction<Integer, GetOrUpdate, PageView, Update>() {

                    private ValueState<Integer> zipCodeState;
                    private ValueState<Deque<Update>> updateBufferState;
                    private ValueState<PriorityQueue<PageView>> pageViewBufferState;

                    @Override
                    public void open(final Configuration parameters) {
                        zipCodeState = getRuntimeContext().getState(zipCodeDescriptor);
                        updateBufferState = getRuntimeContext().getState(updateBufferDescriptor);
                        pageViewBufferState = getRuntimeContext().getState(pageViewBufferDescriptor);
                    }

                    @Override
                    public void processElement1(final GetOrUpdate getOrUpdate,
                                                final Context ctx,
                                                final Collector<Update> out) throws IOException {
                        if (updateBufferState.value() == null) {
                            updateBufferState.update(new ArrayDeque<>());
                        }
                        final var updateBuffer = updateBufferState.value();
                        getOrUpdate.match(
                                get -> null,
                                update -> {
                                    updateBuffer.addLast(update);
                                    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                                    return null;
                                }
                        );
                    }

                    @Override
                    public void processElement2(final PageView pageView,
                                                final Context ctx,
                                                final Collector<Update> out) throws IOException {
                        if (pageViewBufferState.value() == null) {
                            pageViewBufferState.update(new PriorityQueue<>(
                                    Comparator.comparing(TimestampedCore::getPhysicalTimestamp)));
                        }
                        pageViewBufferState.value().add(pageView);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                    }

                    @Override
                    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Update> out) throws Exception {
                        final var updateBuffer = updateBufferState.value();
                        final var pageViewBuffer = pageViewBufferState.value();
                        while (!updateBuffer.isEmpty() &&
                                toEpochMilli(updateBuffer.getFirst().getPhysicalTimestamp()) <= timestamp) {
                            final var update = updateBuffer.removeFirst();
                            while (!pageViewBuffer.isEmpty() &&
                                    pageViewBuffer.element().getPhysicalTimestamp()
                                            .isBefore(update.getPhysicalTimestamp())) {
                                update(pageViewBuffer.remove(), out);
                            }
                            update(update, out);
                        }
                        while (!pageViewBuffer.isEmpty() &&
                                toEpochMilli(pageViewBuffer.element().getPhysicalTimestamp()) <= timestamp) {
                            update(pageViewBuffer.remove(), out);
                        }
                    }

                    private void update(final Update update, final Collector<Update> out) throws IOException {
                        zipCodeState.update(update.getZipCode());
                        out.collect(update);
                    }

                    private void update(final PageView pageView, final Collector<Update> out) throws IOException {
                        if (zipCodeState.value() == null) {
                            // Store some initial value; could be more sophisticated
                            zipCodeState.update(10_000);
                        }
                        // Apart from initializing zip code, this update is a no-op
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
        return (conf.getTotalPageViews() * conf.getPageViewParallelism() +
                conf.getTotalPageViews() / 100 + conf.getTotalPageViews() / 1000) * conf.getTotalUsers();
    }

    @Override
    public long getOptimalThroughput() {
        return (long) ((conf.getPageViewParallelism() + 0.011) * conf.getTotalUsers() * conf.getPageViewRate());
    }

}
