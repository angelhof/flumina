package edu.upenn.flumina;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

public interface Experiment {

    JobExecutionResult run(StreamExecutionEnvironment env, Instant startTime) throws Exception;

    long getTotalEvents();

    long getOptimalThroughput();

}
