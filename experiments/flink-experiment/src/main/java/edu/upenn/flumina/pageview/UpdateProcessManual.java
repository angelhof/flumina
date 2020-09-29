package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class UpdateProcessManual extends KeyedProcessFunction<Integer, Update, Update> {

    private final String rmiHost;
    private final String pageViewServiceName;
    private transient ForkJoinService<Integer, Integer> pageViewService;
    private int zipCode;

    public UpdateProcessManual(final String rmiHost, final String pageViewServiceName) {
        this.rmiHost = rmiHost;
        this.pageViewServiceName = pageViewServiceName;
        this.zipCode = 10_000;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(final Configuration parameters) throws RemoteException, NotBoundException {
        final var registry = LocateRegistry.getRegistry(rmiHost);
        pageViewService = (ForkJoinService<Integer, Integer>) registry.lookup(pageViewServiceName);
    }

    @Override
    public void processElement(final Update update,
                               final Context ctx,
                               final Collector<Update> out) throws RemoteException {
        zipCode = pageViewService.joinParent(getRuntimeContext().getIndexOfThisSubtask(), update.zipCode);
        out.collect(update);
    }

}
