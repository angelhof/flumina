package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.TimestampedUnion;

import java.time.Instant;
import java.util.function.Function;

public class RuleHeartbeat extends Heartbeat implements TimestampedUnion<Rule, RuleHeartbeat> {

    private static final long serialVersionUID = 3962416182988836041L;

    public RuleHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public RuleHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <R> R match(final Function<Rule, R> fstCase, final Function<RuleHeartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
