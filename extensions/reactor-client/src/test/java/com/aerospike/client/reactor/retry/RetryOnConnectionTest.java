package com.aerospike.client.reactor.retry;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.client.reactor.ReactorTest;
import com.aerospike.client.reactor.util.Args;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.client.ResultCode.NO_MORE_CONNECTIONS;
import static com.aerospike.client.reactor.retry.RetryFactories.retryOnNoMoreConnections;

public class RetryOnConnectionTest extends ReactorTest {

    public static final int CALLS_NO = 400;
    private final String binName = args.getBinName("rtronconn");

    public RetryOnConnectionTest(Args args) {
        super(args);
    }

    @Test
    public void shouldFailOnOutOfConnection(){

        String binValue = "value";
        final Bin bin = new Bin(binName, binValue);
        final Key key = new Key(args.namespace, args.set, "failOnConn");

        reactorClient.put(key, bin).block();

        Mono<List<KeyRecord>> monoKeys = Mono.zip(
                IntStream.range(0, CALLS_NO)
                        .mapToObj(value -> reactorClient.get(key))
                        .collect(Collectors.toList()),
                a -> Arrays.asList((KeyRecord[])a));


        StepVerifier.create(monoKeys)
                .expectErrorMatches(throwable -> throwable instanceof AerospikeException.Connection
                        && ((AerospikeException.Connection) throwable).getResultCode() == NO_MORE_CONNECTIONS)
                .verify();
    }

    @Test
    public void shouldRetryOnOutOfConnectionAndSucceed(){

        String binValue = "value";
        final Bin bin = new Bin(binName, binValue);
        final Key key = new Key(args.namespace, args.set, "failOnConn");

        IAerospikeReactorClient retryClient = new AerospikeReactorRetryClient(reactorClient,
                retryOnNoMoreConnections());

        retryClient.put(key, bin).block();

        Mono<List<Object>> monoKeys = Mono.zip(
                IntStream.range(0, CALLS_NO)
                        .mapToObj(value -> retryClient.get(key)
                                .subscribeOn(Schedulers.elastic())
                                .doOnNext(keyRecord -> System.out.println(value)))
                        .collect(Collectors.toList()),
                Arrays::asList);


        StepVerifier.create(monoKeys)
                .expectNextMatches(keys -> keys.size() == CALLS_NO)
                .verifyComplete();
    }

}
