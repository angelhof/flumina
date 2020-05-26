package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;

import java.time.Instant;

public class PageViewSource extends GeneratorWithHeartbeatsBasedSource<PageView, PageViewHeartbeat> {

    private static final long serialVersionUID = 5226831635210275751L;

    public PageViewSource(final int totalEvents, final int totalUsers, final double rate, final Instant startTime) {
        super(new PageViewGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
