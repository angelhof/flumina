package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.GetOrUpdateHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;
import org.apache.flink.configuration.Configuration;

import java.time.Instant;

public class GetOrUpdateSource extends GeneratorWithHeartbeatsBasedSource<GetOrUpdate, GetOrUpdateHeartbeat> {

    private static final long serialVersionUID = 7605642118760769809L;

    private final GetOrUpdateGenerator generator;

    public GetOrUpdateSource(final int totalEvents, final double rate, final Instant startTime) {
        this(new GetOrUpdateGenerator(totalEvents, rate), startTime);
    }

    private GetOrUpdateSource(final GetOrUpdateGenerator generator, final Instant startTime) {
        super(generator, startTime);
        this.generator = generator;
    }

    @Override
    public void open(final Configuration parameters) {
        final var userIds = UserIdHelper.getUserIds(getRuntimeContext().getNumberOfParallelSubtasks());
        final int userId = userIds.get(getRuntimeContext().getIndexOfThisSubtask());
        generator.setUserId(userId);
    }

}
