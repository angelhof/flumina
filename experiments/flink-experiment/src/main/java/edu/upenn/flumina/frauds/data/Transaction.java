package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.TimestampedUnion;

import java.util.function.Function;

public class Transaction extends Heartbeat implements TimestampedUnion<Transaction, TransactionHeartbeat> {

    private static final long serialVersionUID = -7232554583381690665L;

    private final long val;

    public Transaction(final long val, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.val = val;
    }

    public long getVal() {
        return val;
    }

    @Override
    public <R> R match(final Function<Transaction, R> fstCase, final Function<TransactionHeartbeat, R> sndCase) {
        return fstCase.apply(this);
    }

}
