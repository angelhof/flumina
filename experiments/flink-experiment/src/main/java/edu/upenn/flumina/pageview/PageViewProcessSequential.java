package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.Update;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

import static edu.upenn.flumina.time.TimeHelper.toEpochMilli;

public class PageViewProcessSequential extends KeyedCoProcessFunction<Integer, GetOrUpdate, PageView, Update> {

    private static final ValueStateDescriptor<List<Integer>> zipCodeDescriptor =
            new ValueStateDescriptor<>("ZipCode", TypeInformation.of(new TypeHint<>() {
            }));
    private static final ValueStateDescriptor<Queue<Update>> updateBufferDescriptor =
            new ValueStateDescriptor<>("UpdateBuffer", TypeInformation.of(new TypeHint<>() {
            }));
    private static final ValueStateDescriptor<PriorityQueue<PageView>> pageViewBufferDescriptor =
            new ValueStateDescriptor<>("PageViewBuffer", TypeInformation.of(new TypeHint<>() {
            }));

    private transient ValueState<List<Integer>> zipCodeState;
    private transient ValueState<Queue<Update>> updateBufferState;
    private transient ValueState<PriorityQueue<PageView>> pageViewBufferState;

    private final int totalUsers;

    public PageViewProcessSequential(final int totalUsers) {
        this.totalUsers = totalUsers;
    }

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
                    Comparator.comparing(PageView::getPhysicalTimestamp)));
        }
        return pageViewBufferState.value();
    }

    private List<Integer> getZipCodes() throws IOException {
        if (zipCodeState.value() == null) {
            final List<Integer> zipCodes = new ArrayList<>(totalUsers);
            for (int i = 0; i < totalUsers; ++i) {
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

}