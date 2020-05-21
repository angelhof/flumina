package edu.upenn.flumina.pageview;

import edu.upenn.flumina.GeneratorBasedSource;
import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;

public class PageViewSource extends GeneratorBasedSource<PageViewOrHeartbeat> {

    private static final long serialVersionUID = 5226831635210275751L;

    public PageViewSource(final int totalEvents, final int totalUsers, final double rate, final long startTime) {
        super(new PageViewGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
