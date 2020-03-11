package com.aerospike.client.reactor.retry;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.client.reactor.retry.RetryFactories.retryOnNoMoreConnections;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryTest {

    public static final Key KEY = new Key("a", "b", "c");
    public static final Key KEY2 = new Key("a", "b", "c2");
    public static final Key[] KEYS = {KEY, KEY2};
    public static final String[] BIN_NAMES = new String[]{"1", "2"};
    public static final String[] BIN_NAMES2 = new String[]{"3", "4"};

    public static final Bin BIN = new Bin("1", "1");
    public static final Bin BIN2 = new Bin("2", "2");

    public static final List<BatchRead> BATCH = asList(new BatchRead(KEY, BIN_NAMES), new BatchRead(KEY2, BIN_NAMES2));

    public static final AerospikeException.Connection NO_CONNECTION = new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS, "1");
    public static final AerospikeException.Timeout TIMEOUT = new AerospikeException.Timeout(1, false);

    private IAerospikeReactorClient reactorClient = mock(IAerospikeReactorClient.class);

    private IAerospikeReactorClient retryClient = new AerospikeReactorRetryClient(reactorClient,
            retryOnNoMoreConnections());

    @Test
    public void shouldRetryGet(){

        when(reactorClient.get(ArgumentMatchers.any(), ArgumentMatchers.<Key>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetWithBinNames(){

        when(reactorClient.get(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(null, KEY, BIN_NAMES))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchGet(){

        when(reactorClient.get(ArgumentMatchers.any(), ArgumentMatchers.<Key[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatch(){

        when(reactorClient.get(ArgumentMatchers.any(), ArgumentMatchers.<List<BatchRead>>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.get(BATCH))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetFlux(){

        when(reactorClient.getFlux(ArgumentMatchers.any(), ArgumentMatchers.<Key[]>any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getFlux(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchFlux(){

        when(reactorClient.getFlux(ArgumentMatchers.any(), ArgumentMatchers.<List<BatchRead>>any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getFlux(BATCH))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetHeader(){

        when(reactorClient.getHeader(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getHeader(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryGetHeaders(){

        when(reactorClient.getHeaders(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.getHeaders(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryTouch(){

        when(reactorClient.touch(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.touch(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryExists(){

        when(reactorClient.exists(ArgumentMatchers.any(), ArgumentMatchers.<Key>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.exists(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryBatchExists(){

        when(reactorClient.exists(ArgumentMatchers.any(), ArgumentMatchers.<Key[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.exists(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryExistsFlux(){

        when(reactorClient.existsFlux(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.existsFlux(KEYS))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryPut(){

        when(reactorClient.put(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<Bin[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.put(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryAppend(){

        when(reactorClient.append(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<Bin[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.append(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryPrepend(){

        when(reactorClient.prepend(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<Bin[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.prepend(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryAdd(){

        when(reactorClient.add(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<Bin[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.add(KEY, BIN, BIN2))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryDelete(){

        when(reactorClient.delete(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.delete(KEY))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryOperate(){

        when(reactorClient.operate(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<Operation[]>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.operate(KEY, Operation.touch(), Operation.delete()))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryQuery(){

        when(reactorClient.query(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.query(new Statement()))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryScanAll(){

        when(reactorClient.scanAll(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockFluxErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.scanAll("namespace", "setname", BIN_NAMES))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryExecute(){

        when(reactorClient.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(),  ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.execute(KEY, "packageName", "functionName"))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryInfo(){

        when(reactorClient.info(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<String>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.info(null, null, "functionName"))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryInfoList(){

        when(reactorClient.info(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.<List<String>>any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.info(null, null, asList("functionName", "functionName2")))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryCreateIndex(){

        when(reactorClient.createIndex(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.createIndex(null, "ns", "st", "ind", "bb",
                IndexType.NUMERIC, IndexCollectionType.DEFAULT))
                .verifyError(AerospikeException.Timeout.class);
    }

    @Test
    public void shouldRetryDropIndex(){

        when(reactorClient.dropIndex(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any()))
                .thenReturn(mockMonoErrors(NO_CONNECTION, TIMEOUT));

        StepVerifier.create(retryClient.dropIndex(null, "ns", "st", "ind"))
                .verifyError(AerospikeException.Timeout.class);
    }

    private <T> Mono<T> mockMonoErrors(Throwable... errors){
        AtomicInteger subscribeCount = new AtomicInteger();
        return Mono.defer(() ->  Mono.error(errors[subscribeCount.getAndIncrement()]));
    }

    private <T> Flux<T> mockFluxErrors(Throwable... errors){
        AtomicInteger subscribeCount = new AtomicInteger();
        return Flux.defer(() ->  Mono.error(errors[subscribeCount.getAndIncrement()]));
    }
}
