package com.aerospike.client.cluster;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ASThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNumber = new AtomicInteger();

    public final Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "Aerospike-" + threadNumber.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    }
}
