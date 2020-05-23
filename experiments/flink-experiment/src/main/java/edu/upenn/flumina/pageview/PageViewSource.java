package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;

public class PageViewSource extends GeneratorWithHeartbeatsBasedSource<PageView, Heartbeat> {

    private static final long serialVersionUID = 5226831635210275751L;

    public PageViewSource(final int totalEvents, final int totalUsers, final double rate, final long startTime) {
        super(new PageViewGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
