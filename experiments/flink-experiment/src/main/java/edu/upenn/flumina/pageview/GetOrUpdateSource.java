package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;

import java.time.Instant;

public class GetOrUpdateSource extends GeneratorWithHeartbeatsBasedSource<GetOrUpdate, Heartbeat> {

    private static final long serialVersionUID = 7605642118760769809L;

    public GetOrUpdateSource(final int totalEvents, final int totalUsers, final double rate, final Instant startTime) {
        super(new GetOrUpdateGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
