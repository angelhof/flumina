package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Timestamped;

import java.util.function.Function;

public interface GetOrUpdate extends Timestamped {

    interface GetCase<R> extends Function<Get, R> {

    }

    interface UpdateCase<R> extends Function<Update, R> {

    }

    <R> R match(GetCase<R> getCase, UpdateCase<R> updateCase);

    int getUserId();

}
