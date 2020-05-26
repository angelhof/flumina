package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.RuleHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;

import java.time.Instant;

public class RuleSource extends GeneratorWithHeartbeatsBasedSource<Rule, RuleHeartbeat> {

    private static final long serialVersionUID = 670824240112384376L;

    public RuleSource(final int totalTransactions,
                      final double transactionRate,
                      final int trRatio,
                      final int hbRatio,
                      final Instant startTime) {
        super(new RuleGenerator(totalTransactions, transactionRate, trRatio, hbRatio), startTime);
    }

}
