package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

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

        getOrUpdateStream.keyBy(gou -> 0)
                .connect(pageViewStream.keyBy(pv -> 0))
                .process(new PageViewProcessSequential(conf.getTotalUsers()))
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
