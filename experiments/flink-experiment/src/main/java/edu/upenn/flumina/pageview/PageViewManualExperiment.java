package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.PageViewConfig;
import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.remote.ForkJoinService;
import edu.upenn.flumina.util.FlinkHashInverter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.time.Instant;
import java.util.UUID;

public class PageViewManualExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewManualExperiment.class);

    private final PageViewConfig conf;

    public PageViewManualExperiment(final PageViewConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);

        final int pageViewParallelism = conf.getPageViewParallelism();

        // Set up the remote ForkJoin service
        final var pageViewService = new PageViewService(conf.getTotalUsers(), pageViewParallelism);
        @SuppressWarnings("unchecked") final var pageViewServiceStub =
                (ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>>) UnicastRemoteObject.exportObject(pageViewService, 0);
        final var pageViewServiceName = UUID.randomUUID().toString();
        final var registry = LocateRegistry.getRegistry(conf.getRmiHost());
        registry.rebind(pageViewServiceName, pageViewServiceStub);

        final var getOrUpdateSource = new GetOrUpdateOrHeartbeatSource(conf.getTotalPageViews(),
                conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var getOrUpdateStream = env.addSource(getOrUpdateSource)
                .slotSharingGroup("getOrUpdate");
        final var pageViewSource =
                new PageViewOrHeartbeatSource(conf.getTotalPageViews(), conf.getTotalUsers(), conf.getPageViewRate(), startTime);
        final var pageViewStream = env.addSource(pageViewSource)
                .setParallelism(pageViewParallelism);

        final var broadcastStateDescriptor = new MapStateDescriptor<>("Dummy", Void.class, Void.class);
        final var broadcastGetOrUpdateStream =
                getOrUpdateStream.broadcast(broadcastStateDescriptor);

        // We use the key inverter to correctly distribute events over parallel instances. There are
        // totalUsers * pageViewParallelism processing subtasks, and we want a pageView with given userId
        // and subtaskIndex (determined by the pageViewStream instance) to be processed by subtask with
        // with index userId * pageViewParallelism + subtaskIndex. The inverter inverts this index to a
        // key that, once hashed, will map back to the index.
        final var keyInverter =
                FlinkHashInverter.getMapping(conf.getTotalUsers() * pageViewParallelism);
        pageViewStream.keyBy(poh -> keyInverter.get(poh.getUserId() * pageViewParallelism + poh.getSourceIndex()))
                .connect(broadcastGetOrUpdateStream)
                .process(new PageViewProcessManual(conf.getRmiHost(), pageViewServiceName, pageViewParallelism))
                .setParallelism(conf.getTotalUsers() * pageViewParallelism);

        // We again use an inverter to invert user IDs
        final var invertedUserIds = FlinkHashInverter.getMapping(conf.getTotalUsers());
        getOrUpdateStream
                .flatMap(new FlatMapFunction<GetOrUpdateOrHeartbeat, Update>() {
                    @Override
                    public void flatMap(final GetOrUpdateOrHeartbeat getOrUpdateOrHeartbeat, final Collector<Update> out) {
                        getOrUpdateOrHeartbeat.match(
                                getOrUpdate -> getOrUpdate.match(
                                        get -> null,
                                        update -> {
                                            out.collect(update);
                                            return null;
                                        }
                                ),
                                heartbeat -> null
                        );
                    }
                })
                .keyBy(update -> invertedUserIds.get(update.getUserId()))
                .process(new UpdateProcessManual(conf.getRmiHost(), pageViewServiceName))
                .setParallelism(conf.getTotalUsers())
                .map(new TimestampMapper())
                .setParallelism(conf.getTotalUsers())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

        try {
            return env.execute("PageView Experiment");
        } finally {
            UnicastRemoteObject.unexportObject(pageViewService, true);
            try {
                registry.unbind(pageViewServiceName);
            } catch (final NotBoundException ignored) {

            }
        }
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
