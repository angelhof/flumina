package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.TimestampedUnion;

import java.time.Instant;
import java.util.function.Function;

public class TransactionHeartbeat extends Heartbeat implements TimestampedUnion<Transaction, TransactionHeartbeat> {

    private static final long serialVersionUID = -9178399979187778858L;

    // Default constructor so that the object is treated like POJO
    public TransactionHeartbeat() {

    }

    public TransactionHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public TransactionHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <R> R match(final Function<Transaction, R> fstCase, final Function<TransactionHeartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
