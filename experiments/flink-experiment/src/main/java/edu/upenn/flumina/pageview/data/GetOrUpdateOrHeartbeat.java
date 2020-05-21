package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.Timestamped;

import java.util.function.Function;

public interface GetOrUpdateOrHeartbeat extends Timestamped {

    <T> T match(Function<Get, T> getCase, Function<Update, T> updateCase, HeartbeatGUHCase<T> heartbeatCase);

}
