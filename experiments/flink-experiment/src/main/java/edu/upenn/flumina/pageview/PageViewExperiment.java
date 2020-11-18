package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.pageview.data.GetOrUpdateHeartbeat;
import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.util.FlinkHashInverter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PageViewExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewExperiment.class);

    private final PageViewConfig conf;

    public PageViewExperiment(final PageViewConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);

        final var getOrUpdateSource = new GetOrUpdateOrHeartbeatSource(conf.getTotalPageViews(),
                conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var getOrUpdateStream = env.addSource(getOrUpdateSource)
                .slotSharingGroup("getOrUpdate");
        final var pageViewSource = new PageViewOrHeartbeatSource(conf.getTotalPageViews(),
                conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var pageViewStream = env.addSource(pageViewSource)
                .setParallelism(conf.getPageViewParallelism());

        // We need to duplicate the GetOrUpdate heartbeats, so that each processing instance gets a copy
        final var totalUsers = conf.getTotalUsers();
        final var gouWithDupHeartbeats = getOrUpdateStream
                .flatMap(new FlatMapFunction<GetOrUpdateOrHeartbeat, GetOrUpdateOrHeartbeat>() {

                    @Override
                    public void flatMap(final GetOrUpdateOrHeartbeat getOrUpdateOrHeartbeat,
                                        final Collector<GetOrUpdateOrHeartbeat> out) {
                        final List<GetOrUpdateOrHeartbeat> toCollect = getOrUpdateOrHeartbeat.match(
                                gou -> gou.match(List::of, List::of),
                                hb -> IntStream.range(0, totalUsers)
                                        .mapToObj(userId -> new GetOrUpdateHeartbeat(hb, userId))
                                        .collect(Collectors.toList())
                        );
                        toCollect.forEach(out::collect);
                    }
                });

        // Normal low-level join
        // We invert the key so that each event is routed to a correct parallel processing instance
        final var invertedUserIds = FlinkHashInverter.getMapping(conf.getTotalUsers());
        gouWithDupHeartbeats.keyBy(gou -> invertedUserIds.get(gou.getUserId()))
                .connect(pageViewStream.keyBy(pv -> invertedUserIds.get(pv.getUserId())))
                .process(new PageViewProcessParallel(conf.getPageViewParallelism()))
                .setParallelism(conf.getTotalUsers())
                .map(new TimestampMapper())
                .setParallelism(conf.getTotalUsers())
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
