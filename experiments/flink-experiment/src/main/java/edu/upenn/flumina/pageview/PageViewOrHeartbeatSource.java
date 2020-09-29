package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;
import edu.upenn.flumina.source.GeneratorBasedSource;

import java.time.Instant;

public class PageViewOrHeartbeatSource extends GeneratorBasedSource<PageViewOrHeartbeat> {

    private static final long serialVersionUID = 2058624344019111587L;

    public PageViewOrHeartbeatSource(final int totalEvents, final int totalUsers, final double rate, final Instant startTime) {
        super(new PageViewGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
