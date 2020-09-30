package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.TransactionOrHeartbeat;
import edu.upenn.flumina.source.GeneratorBasedSource;

import java.time.Instant;

public class TransactionOrHeartbeatSource extends GeneratorBasedSource<TransactionOrHeartbeat> {

    private static final long serialVersionUID = -4339350180747606563L;

    public TransactionOrHeartbeatSource(final int totalTransactions, final double rate, final Instant startTime) {
        super(new TransactionGenerator(totalTransactions, rate), startTime);
    }

}
