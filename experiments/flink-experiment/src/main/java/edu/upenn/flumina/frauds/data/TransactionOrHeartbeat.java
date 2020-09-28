package edu.upenn.flumina.frauds.data;

import edu.upenn.flumina.data.TimestampedUnion;

public interface TransactionOrHeartbeat extends TimestampedUnion<Transaction, TransactionHeartbeat> {

}
