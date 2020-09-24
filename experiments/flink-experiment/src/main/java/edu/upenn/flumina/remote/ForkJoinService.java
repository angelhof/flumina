package edu.upenn.flumina.remote;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ForkJoinService<S extends Serializable> extends Remote {

    int getChildId() throws RemoteException;

    S joinChild(int childId, S state) throws RemoteException;

    S joinParent() throws RemoteException;

}
