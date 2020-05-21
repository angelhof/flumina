package edu.upenn.flumina.pageview;

import edu.upenn.flumina.GeneratorBasedSource;
import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;

public class GetOrUpdateSource extends GeneratorBasedSource<GetOrUpdateOrHeartbeat> {

    private static final long serialVersionUID = 7605642118760769809L;

    public GetOrUpdateSource(final int totalEvents, final int totalUsers, final double rate, final long startTime) {
        super(new GetOrUpdateGenerator(totalEvents, totalUsers, rate), startTime);
    }

}
