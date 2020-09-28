package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.Heartbeat;

import java.time.Instant;
import java.util.function.Function;

public class RuleHeartbeat extends Heartbeat implements RuleOrHeartbeat {

    private static final long serialVersionUID = 3962416182988836041L;

    // Default constructor so that the object is treated like POJO
    public RuleHeartbeat() {

    }

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

    @Override
    public String toString() {
        return "RuleHeartbeat{" +
                "logicalTimestamp=" + logicalTimestamp +
                ", sourceIndex=" + sourceIndex +
                '}';
    }

}
