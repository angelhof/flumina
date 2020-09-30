package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.source.GeneratorBasedSource;

import java.time.Instant;

public class GetOrUpdateOrHeartbeatSource extends GeneratorBasedSource<GetOrUpdateOrHeartbeat> {

    private static final long serialVersionUID = -6850918498828248882L;

    public GetOrUpdateOrHeartbeatSource(final int totalEvents, final int totalUsers, final double rate, final Instant startTime) {
        super(new GetOrUpdateGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
