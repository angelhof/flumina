package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.RuleOrHeartbeat;
import edu.upenn.flumina.source.GeneratorBasedSource;

import java.time.Instant;

public class RuleOrHeartbeatSource extends GeneratorBasedSource<RuleOrHeartbeat> {

    private static final long serialVersionUID = -541308116310189273L;

    public RuleOrHeartbeatSource(final int totalTransactions,
                                 final double transactionRate,
                                 final int trRatio,
                                 final int hbRatio,
                                 final Instant startTime) {
        super(new RuleGenerator(totalTransactions, transactionRate, trRatio, hbRatio), startTime);
    }

}
