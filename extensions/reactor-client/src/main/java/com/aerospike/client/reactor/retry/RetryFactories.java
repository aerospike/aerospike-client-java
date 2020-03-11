package com.aerospike.client.reactor.retry;

import com.aerospike.client.AerospikeException;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.aerospike.client.ResultCode.NO_MORE_CONNECTIONS;

public class RetryFactories {

    private RetryFactories(){}

    private static final Random random = new Random();

    public static Function<Flux<Throwable>, ? extends Publisher<?>> retryOnNoMoreConnections(){
        return retryOn(throwable -> throwable instanceof AerospikeException.Connection
                      && ((AerospikeException.Connection) throwable).getResultCode() == NO_MORE_CONNECTIONS);
    }

    public static Function<Flux<Throwable>, ? extends Publisher<?>> retryOn(Predicate<Throwable> retryOn){
        AtomicLong backOff = new AtomicLong();
        return retry((throwable, integer) -> retryOn.test(throwable)
                ? backOff.addAndGet(random.nextInt(10))
                : -1);
    }

    public static Function<Flux<Throwable>, ? extends Publisher<?>> retry(BiFunction<Throwable, Integer, Long> retryDelay){
        return throwableFlux -> throwableFlux
                .zipWith(Flux.range(1, Integer.MAX_VALUE), (error, index) -> {
                    long delay = retryDelay.apply(error, index);
                    if (delay >= 0) {
                        return Tuples.of(delay, error);
                    } else {
                        throw Exceptions.propagate(error);
                    }
                }).concatMap(
                        tuple2 -> tuple2.getT1() > 0
                                ? Mono.delay(Duration.ofMillis(tuple2.getT1()))
                                .map(time -> tuple2.getT2())
                                : Mono.just(tuple2.getT2()));
    }

}
