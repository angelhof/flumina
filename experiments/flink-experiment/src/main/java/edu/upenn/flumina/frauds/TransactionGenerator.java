package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Transaction;
import edu.upenn.flumina.frauds.data.TransactionHeartbeat;
import edu.upenn.flumina.frauds.data.TransactionOrHeartbeat;
import edu.upenn.flumina.source.Generator;

import java.time.Instant;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TransactionGenerator implements Generator<TransactionOrHeartbeat> {

    private static final long serialVersionUID = 827769633731286027L;

    private final int totalTransactions;
    private final double rate;

    public TransactionGenerator(final int totalTransactions, final double rate) {
        this.totalTransactions = totalTransactions;
        this.rate = rate;
    }

    @Override
    public double getRate() {
        return rate;
    }

    @Override
    public Iterator<TransactionOrHeartbeat> getIterator() {
        // Prepare a stream of Transaction objects with timestamps ranging from 0 to totalValues-1.
        // Add one heartbeat with timestamp totalTransactions at the end.
        final var transactions =
                LongStream.range(0, totalTransactions).<TransactionOrHeartbeat>mapToObj(t -> new Transaction(t + 1, t));
        final var withFinalHeartbeat =
                Stream.concat(transactions, Stream.of(new TransactionHeartbeat(totalTransactions, Instant.MAX)));
        return withFinalHeartbeat.iterator();
    }

}
