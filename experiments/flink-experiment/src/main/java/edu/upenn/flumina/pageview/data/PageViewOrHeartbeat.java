package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.Timestamped;

import java.util.function.Function;

public interface PageViewOrHeartbeat extends Timestamped {

    <T> T match(Function<PageView, T> pageViewCase, HeartbeatPVHCase<T> heartbeatCase);

}
