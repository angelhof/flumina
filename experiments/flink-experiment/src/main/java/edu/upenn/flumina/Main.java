package edu.upenn.flumina;

import edu.upenn.flumina.config.Config;
import edu.upenn.flumina.config.ConfigException;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.config.ValueBarrierConfig;
import edu.upenn.flumina.pageview.PageViewExperiment;
import edu.upenn.flumina.valuebarrier.ValueBarrierExperiment;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static void run(final Experiment experiment, final Config conf) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // By default, the buffer timeout is 100ms, resulting in median latencies around 100ms.
        // Since Flumina doesn't have the corresponding mechanism for tuning the latency vs. throughput tradeoff,
        // we set this to 0 -- events are immediately flushed from the buffer.
        env.setBufferTimeout(0);

        // We sync all the input sources with the same start time
        final long startTime = System.nanoTime() + conf.getInitSyncDelay();

        final JobExecutionResult result = experiment.run(env, startTime);

        try (final FileWriter statsFile = new FileWriter(conf.getStatsFile())) {
            final long totalEvents = experiment.getTotalEvents();
            final long totalTimeMillis = (System.nanoTime() - startTime) / 1_000_000;
            final long netRuntimeMillis = result.getNetRuntime(TimeUnit.MILLISECONDS);
            final long meanThroughput = Math.floorDiv(totalEvents, netRuntimeMillis);
            final long optimalThroughput = experiment.getOptimalThroughput();
            statsFile.write(String.format("Total time (ms): %d%n", totalTimeMillis));
            statsFile.write(String.format("Events processed: %d%n", totalEvents));
            statsFile.write(String.format("Net runtime (ms): %d%n", netRuntimeMillis));
            statsFile.write(String.format("Mean throughput (events/ms): %d%n", meanThroughput));
            statsFile.write(String.format("Optimal throughput (events/ms): %d%n", optimalThroughput));
        } catch (final IOException e) {
            LOG.error("Exception while trying to write to {}: {}", conf.getStatsFile(), e.getMessage());
        }
    }

    public static void main(final String[] args) throws Exception {
        try {
            final Config conf = Config.fromArgs(args);
            if (conf instanceof ValueBarrierConfig) {
                final ValueBarrierConfig valueBarrierConf = (ValueBarrierConfig) conf;
                final ValueBarrierExperiment valueBarrierExperiment = new ValueBarrierExperiment(valueBarrierConf);
                run(valueBarrierExperiment, conf);
            } else if (conf instanceof PageViewConfig) {
                final PageViewConfig pageViewConf = (PageViewConfig) conf;
                final PageViewExperiment pageViewExperiment = new PageViewExperiment(pageViewConf);
                run(pageViewExperiment, conf);
            }
        } catch (final ConfigException e) {
            LOG.error("Configuration error: {}", e.getMessage());
        }
    }

}