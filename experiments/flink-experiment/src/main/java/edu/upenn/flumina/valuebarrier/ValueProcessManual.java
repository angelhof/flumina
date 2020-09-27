package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.remote.ForkJoinService;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Queue;

import static edu.upenn.flumina.time.TimeHelper.max;
import static edu.upenn.flumina.time.TimeHelper.min;

public class ValueProcessManual extends BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Void> {

    private Instant valuePhysicalTimestamp = Instant.MIN;
    private Instant barrierPhysicalTimestamp = Instant.MIN;
    private long sum = 0;

    private final Queue<Value> unprocessedValues = new ArrayDeque<>();
    private final Queue<Barrier> unprocessedBarriers = new ArrayDeque<>();

    private final String rmiHost;
    private final String valueBarrierServiceName;
    private transient ForkJoinService<Long> valueBarrierService;
    private transient int id;

    public ValueProcessManual(final String rmiHost, final String valueBarrierServiceName) {
        this.rmiHost = rmiHost;
        this.valueBarrierServiceName = valueBarrierServiceName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(final Configuration parameters) throws RemoteException, NotBoundException {
        final var registry = LocateRegistry.getRegistry(rmiHost);
        valueBarrierService = (ForkJoinService<Long>) registry.lookup(valueBarrierServiceName);
        id = valueBarrierService.getChildId();
    }

    @Override
    public void processElement(final ValueOrHeartbeat valueOrHeartbeat,
                               final ReadOnlyContext ctx,
                               final Collector<Void> out) throws RemoteException {
        valueOrHeartbeat.match(
                value -> {
                    unprocessedValues.add(value);
                    return null;
                },
                heartbeat -> null
        );
        valuePhysicalTimestamp = max(valuePhysicalTimestamp, valueOrHeartbeat.getPhysicalTimestamp());
        makeProgress();
    }

    @Override
    public void processBroadcastElement(final BarrierOrHeartbeat barrierOrHeartbeat,
                                        final Context ctx,
                                        final Collector<Void> out) throws RemoteException {
        barrierOrHeartbeat.match(
                barrier -> {
                    unprocessedBarriers.add(barrier);
                    return null;
                },
                heartbeat -> null
        );
        barrierPhysicalTimestamp = max(barrierPhysicalTimestamp, barrierOrHeartbeat.getPhysicalTimestamp());
        makeProgress();
    }

    private void makeProgress() throws RemoteException {
        final var currentTime = min(valuePhysicalTimestamp, barrierPhysicalTimestamp);
        while (!unprocessedValues.isEmpty() &&
                unprocessedValues.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
            final var value = unprocessedValues.remove();
            while (!unprocessedBarriers.isEmpty() &&
                    unprocessedBarriers.element().getPhysicalTimestamp().isBefore(value.getPhysicalTimestamp())) {
                join(unprocessedBarriers.remove());
            }
            update(value);
        }
        while (!unprocessedBarriers.isEmpty() &&
                unprocessedBarriers.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
            join(unprocessedBarriers.remove());
        }
    }

    private void update(final Value value) {
        sum += value.val;
    }

    private void join(final Barrier barrier) throws RemoteException {
        sum = valueBarrierService.joinChild(id, sum);
    }

}
