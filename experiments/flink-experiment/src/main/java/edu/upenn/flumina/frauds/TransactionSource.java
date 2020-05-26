package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Transaction;
import edu.upenn.flumina.frauds.data.TransactionHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;

import java.time.Instant;

public class TransactionSource extends GeneratorWithHeartbeatsBasedSource<Transaction, TransactionHeartbeat> {

    private static final long serialVersionUID = -8958448863511168499L;

    public TransactionSource(final int totalTransactions, final double rate, final Instant startTime) {
        super(new TransactionGenerator(totalTransactions, rate), startTime);
    }

}
