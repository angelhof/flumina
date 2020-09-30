package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.TimestampedUnion;

public interface RuleOrHeartbeat extends TimestampedUnion<Rule, RuleHeartbeat> {

}
