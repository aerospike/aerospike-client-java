package com.aerospike.client.reactor.util;

import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

public class ReactorUtil {

	public static <T> Mono<T> succeedAfterRetries(Mono<T> mono, AtomicInteger failsCount, Throwable t){
		return Mono.subscriberContext()
				.flatMap(context -> {
					if(failsCount.get() > 0){
						failsCount.decrementAndGet();
						return Mono.error(t);
					} else {
						return mono;
					}
				});
	}

}
