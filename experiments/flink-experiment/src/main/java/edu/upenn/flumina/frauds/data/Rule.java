package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.TimestampedUnion;

import java.util.function.Function;

public class Rule extends Heartbeat implements TimestampedUnion<Rule, RuleHeartbeat> {

    private static final long serialVersionUID = -882844082385911345L;

    // Default constructor so that the object is treated like POJO
    public Rule() {

    }

    public Rule(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    @Override
    public <R> R match(final Function<Rule, R> fstCase, final Function<RuleHeartbeat, R> sndCase) {
        return fstCase.apply(this);
    }

}
