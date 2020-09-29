package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.data.Heartbeat;
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
import java.util.*;

import static edu.upenn.flumina.time.TimeHelper.toEpochMilli;

public class PageViewSequentialExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewSequentialExperiment.class);

    private final PageViewConfig conf;

    public PageViewSequentialExperiment(final PageViewConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var getOrUpdateSource = new GetOrUpdateSource(conf.getTotalPageViews(),
                conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var getOrUpdateStream = env.addSource(getOrUpdateSource)
                .slotSharingGroup("getOrUpdate");
        final var pageViewSource =
                new PageViewSource(conf.getTotalPageViews(), conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var pageViewStream = env.addSource(pageViewSource)
                .setParallelism(conf.getPageViewParallelism());

        final var zipCodeDescriptor = new ValueStateDescriptor<>("ZipCode",
                TypeInformation.of(new TypeHint<List<Integer>>() {
                }));
        final var updateBufferDescriptor = new ValueStateDescriptor<>("UpdateBuffer",
                TypeInformation.of(new TypeHint<Queue<Update>>() {
                }));
        final var pageViewBufferDescriptor = new ValueStateDescriptor<>("PageViewBuffer",
                TypeInformation.of(new TypeHint<PriorityQueue<PageView>>() {
                }));

        getOrUpdateStream.keyBy(gou -> 0)
                .connect(pageViewStream.keyBy(pv -> 0))
                .process(new KeyedCoProcessFunction<Integer, GetOrUpdate, PageView, Update>() {

                    private ValueState<List<Integer>> zipCodeState;
                    private ValueState<Queue<Update>> updateBufferState;
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
                        final var updateBuffer = getUpdateBuffer();
                        getOrUpdate.match(
                                get -> null,
                                update -> {
                                    updateBuffer.add(update);
                                    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                                    return null;
                                }
                        );
                    }

                    @Override
                    public void processElement2(final PageView pageView,
                                                final Context ctx,
                                                final Collector<Update> out) throws IOException {
                        getPageViewBuffer().add(pageView);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                    }

                    @Override
                    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Update> out) throws Exception {
                        final var updateBuffer = getUpdateBuffer();
                        final var pageViewBuffer = getPageViewBuffer();

                        while (!updateBuffer.isEmpty() &&
                                toEpochMilli(updateBuffer.element().getPhysicalTimestamp()) <= timestamp) {
                            final var update = updateBuffer.remove();
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

                    private Queue<Update> getUpdateBuffer() throws IOException {
                        if (updateBufferState.value() == null) {
                            updateBufferState.update(new ArrayDeque<>());
                        }
                        return updateBufferState.value();
                    }

                    private PriorityQueue<PageView> getPageViewBuffer() throws IOException {
                        if (pageViewBufferState.value() == null) {
                            pageViewBufferState.update(new PriorityQueue<>(
                                    Comparator.comparing(Heartbeat::getPhysicalTimestamp)));
                        }
                        return pageViewBufferState.value();
                    }

                    private List<Integer> getZipCodes() throws IOException {
                        if (zipCodeState.value() == null) {
                            final List<Integer> zipCodes = new ArrayList<>(conf.getTotalUsers());
                            for (int i = 0; i < conf.getTotalUsers(); ++i) {
                                zipCodes.add(10_000);
                            }
                            zipCodeState.update(zipCodes);
                        }
                        return zipCodeState.value();
                    }

                    private void update(final Update update, final Collector<Update> out) throws IOException {
                        getZipCodes().set(update.getUserId(), update.zipCode);
                        out.collect(update);
                    }

                    private void update(final PageView pageView, final Collector<Update> out) {
                        // This update is a no-op
                    }
                })
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

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
