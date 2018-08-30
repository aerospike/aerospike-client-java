package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import org.rnorth.ducttape.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.rnorth.ducttape.unreliables.Unreliables.retryUntilSuccess;

public class AerospikeWaitStrategy extends AbstractWaitStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String host;
    private final int port;

    public AerospikeWaitStrategy(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    protected void waitUntilReady() {
        log.info("Waiting for {} seconds for Aerospike to startup", startupTimeout.getSeconds());

        // try to connect to the URL
        try {
            retryUntilSuccess((int) startupTimeout.getSeconds(), TimeUnit.SECONDS, () -> {
                getRateLimiter().doWhenReady(() -> {
                    boolean isConnected = false;
                    try (AerospikeClient client = new AerospikeClient(host, port)) {
                        isConnected = client.isConnected();
                    } catch (AerospikeException.Connection e) {
                        log.debug("Aerospike not yet started. {}", e.getMessage());
                    }
                    if (!isConnected) {
                        throw new ContainerLaunchException("Aerospike not ready yet");
                    }

                });
                return true;
            });
        } catch (TimeoutException e) {
            throw new ContainerLaunchException("Timed out waiting for Aerospike");
        }
    }

}
