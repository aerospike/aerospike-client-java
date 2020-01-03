package com.aerospike.client.reactor;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.reactor.util.Args;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ReactorInfoTest extends ReactorTest{

    public ReactorInfoTest(Args args) {
        super(args);
    }

    @Test
    public void shouldQueryInfoCommand() {

        Mono<String> mono = reactorClient.info(null, null, "namespaces");

        StepVerifier.create(mono)
                .expectNext("test")
                .verifyComplete();
    }

    @Test
    public void shouldFailOnUnknownCommand() {

        Mono<String> mono = reactorClient.info(null, null, "XXX");

        StepVerifier.create(mono)
                .expectErrorMatches(throwable -> throwable instanceof AerospikeException
                        && throwable.getMessage().equals("Error -1: Unknown info command: [XXX]"))
                .verify();
    }

}
