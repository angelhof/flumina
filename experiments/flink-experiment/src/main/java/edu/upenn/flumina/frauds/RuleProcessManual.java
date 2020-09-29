package edu.upenn.flumina.frauds;

import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Instant;

public class RuleProcessManual extends ProcessFunction<Rule, Tuple3<String, Long, Instant>> {

    private final String rmiHost;
    private final String fraudDetectionServiceName;
    private transient ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>> fraudDetectionService;

    private Tuple2<Long, Long> previousAndCurrentSum;

    public RuleProcessManual(final String rmiHost, final String fraudDetectionServiceName) {
        this.rmiHost = rmiHost;
        this.fraudDetectionServiceName = fraudDetectionServiceName;
        this.previousAndCurrentSum = Tuple2.of(0L, 0L);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(final Configuration parameters) throws RemoteException, NotBoundException {
        final var registry = LocateRegistry.getRegistry(rmiHost);
        fraudDetectionService =
                (ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>>) registry.lookup(fraudDetectionServiceName);
    }

    @Override
    public void processElement(final Rule rule,
                               final Context ctx,
                               final Collector<Tuple3<String, Long, Instant>> out) throws RemoteException {
        previousAndCurrentSum = fraudDetectionService.joinParent(
                getRuntimeContext().getIndexOfThisSubtask(), previousAndCurrentSum);
        out.collect(Tuple3.of("Rule", previousAndCurrentSum.f1, rule.getPhysicalTimestamp()));
    }

}
