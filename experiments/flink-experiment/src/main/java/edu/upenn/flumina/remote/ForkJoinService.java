package edu.upenn.flumina.remote;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ForkJoinService<C extends Serializable, P extends Serializable> extends Remote {

    C joinChild(int subtaskIndex, C state) throws RemoteException;

    P joinParent(int subtaskIndex, P state) throws RemoteException;

}
