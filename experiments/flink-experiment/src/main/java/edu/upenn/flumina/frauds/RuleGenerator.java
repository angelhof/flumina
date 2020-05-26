package edu.upenn.flumina.frauds;

import edu.upenn.flumina.data.TimestampedUnion;
import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.RuleHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.time.Instant;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class RuleGenerator implements GeneratorWithHeartbeats<Rule, RuleHeartbeat> {

    private static final long serialVersionUID = -6359817358408850880L;

    private final int totalTransactions;
    private final double transactionRate;
    private final int trRatio;
    private final int hbRatio;

    public RuleGenerator(final int totalTransactions, final double transactionRate, final int trRatio, final int hbRatio) {
        this.totalTransactions = totalTransactions;
        this.transactionRate = transactionRate;
        this.trRatio = trRatio;
        this.hbRatio = hbRatio;
    }

    @Override
    public double getRate() {
        return transactionRate;
    }

    @Override
    public Iterator<TimestampedUnion<Rule, RuleHeartbeat>> getIterator() {
        // There should be one rule for every batch of (totalTransactions / trRatio) transactions.
        // Furthermore, for every rule there are (hbRatio-1) heartbeats, i.e., for every
        // rule there are hbRatio rules or heartbeats.
        //
        // For example, let totalTransactions = 100, trRatio = 10, hbRatio = 5. There should
        // be 100 / 10 = 10 rules, and 10 * 5 = 50 rules and heartbeats.
        // The stream with timestamps should be as follows:
        //
        //   hb(1), hb(3), hb(5), hb(7), rule(9),
        //     ...,
        //       hb(91), hb(93), hb(95), hb(97), rule(99)
        //
        // We add one heartbeat with timestamp totalTransactions at the end.
        final int totalRulesOrHeartbeats = totalTransactions / trRatio * hbRatio;
        final Stream<TimestampedUnion<Rule, RuleHeartbeat>> rules = LongStream.rangeClosed(1, totalRulesOrHeartbeats)
                .mapToObj(i -> {
                    if (i % hbRatio == 0) {
                        // Multiply trRatio / hbRatio first to avoid potential overflow.
                        // Assumes that hbRatio divides trRatio.
                        return new Rule(i * (trRatio / hbRatio) - 1);
                    } else {
                        return new RuleHeartbeat(i * (trRatio / hbRatio) - 1);
                    }
                });
        final var withFinalHeartbeat =
                Stream.concat(rules, Stream.of(new RuleHeartbeat(totalTransactions, Instant.MAX)));
        return withFinalHeartbeat.iterator();
    }

}
