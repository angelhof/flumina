package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.remote.ForkJoinService;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Instant;

public class BarrierProcessManual extends ProcessFunction<Barrier, Tuple3<Long, Long, Instant>> {

    private final String rmiHost;
    private final String valueBarrierServiceName;
    private transient ForkJoinService<Long> valueBarrierService;

    public BarrierProcessManual(final String rmiHost, final String valueBarrierServiceName) {
        this.rmiHost = rmiHost;
        this.valueBarrierServiceName = valueBarrierServiceName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(final Configuration parameters) throws RemoteException, NotBoundException {
        final var registry = LocateRegistry.getRegistry(rmiHost);
        valueBarrierService = (ForkJoinService<Long>) registry.lookup(valueBarrierServiceName);
    }

    @Override
    public void processElement(final Barrier barrier,
                               final Context ctx,
                               final Collector<Tuple3<Long, Long, Instant>> out) throws RemoteException {
        final long sum = valueBarrierService.joinParent();
        out.collect(Tuple3.of(sum, barrier.getLogicalTimestamp(), barrier.getPhysicalTimestamp()));
    }

}
