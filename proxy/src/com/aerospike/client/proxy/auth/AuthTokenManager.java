/*
 * Copyright 2012-2023 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.proxy.auth;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.proxy.auth.credentials.BearerTokenCallCredentials;
import com.aerospike.client.proxy.grpc.GrpcChannelProvider;
import com.aerospike.proxy.client.Auth;
import com.aerospike.proxy.client.AuthServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.CallOptions;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;

/**
 * An access token manager for Aerospike proxy.
 */
public class AuthTokenManager implements Closeable {
    /**
     * A conservative estimate of minimum amount of time in millis it takes for
     * token refresh to complete. Auto refresh should be scheduled at least
     * this amount before expiry, i.e, ff remaining expiry time is less than
     * this amount refresh should be scheduled immediately.
     */
    private static final int refreshMinTime = 5000;

    /**
     * A cap on refresh time in millis to throttle an auto refresh requests in
     * case of token refresh failure.
     */
    private static final int maxExponentialBackOff = 15000;

    /**
     * Fraction of token expiry time to elapse before scheduling an auto
     * refresh.
     *
     * @see AuthTokenManager#refreshMinTime
     */
    private static final float refreshAfterFraction = 0.95f;

    /**
     * An {@link ObjectMapper} to parse access token.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ClientPolicy clientPolicy;
    private final GrpcChannelProvider channelProvider;
    private final ScheduledExecutorService executor;
    private final Lock tokenFetchLock;
    private final AccessToken accessToken;
    private volatile boolean fetchScheduled;

    public AuthTokenManager(ClientPolicy clientPolicy, GrpcChannelProvider grpcCallExecutor) {
        this.clientPolicy = clientPolicy;
        this.channelProvider = grpcCallExecutor;
        this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("auth-manager").build());
        this.accessToken = new AccessToken();
        this.tokenFetchLock = new ReentrantLock();
        fetchToken(true);
    }

    public CallOptions setCallCredentials(
            CallOptions callOptions) {
        if (isTokenRequired()) {
            if (isTokenInValid()) {
                if (Log.warnEnabled()) {
                    // TODO: This warns for evey call, spamming the output.
                    //  Should be rate limited. Possibly once in a few seconds.
                    // This alerts that auto refresh didn't finish correctly. In normal scenario, this should never
                    // happen.
                    Log.warn("Trying to refresh token before setting into call");
                }
                unsafeScheduleRefresh(0, false);
                try {
                    // TODO: Maybe thread sleep is not a great idea.
                    // Give some time for the refresh to complete.
                    Thread.sleep(refreshMinTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (isTokenInValid()) {
                throw new IllegalStateException("Access token has expired");
            }
            return callOptions.withCallCredentials(new BearerTokenCallCredentials(accessToken.token));
        }
        return callOptions;
    }

    /**
     * Fetch the new token if expired or scheduled for auto refresh.
     *
     * @param forceRefresh A boolean flag to refresh token forcefully. This is required for initialization and auto
     *                     refresh. Auto refresh will get rejected as token won't be expired at that time, but we need
     *                     to refresh it beforehand. If true, this function will run from the <b>invoking thread</b>,
     *                     not from the scheduler.
     */
    private void fetchToken(boolean forceRefresh) {
        fetchScheduled = false;
        if (!isTokenRequired()) {
            return;
        }
        if (shouldRefresh(forceRefresh)) {
            tokenFetchLock.lock();
            if (shouldRefresh(forceRefresh)) {
                try {
                    if (Log.debugEnabled()) {
                        Log.debug("Starting token refresh");
                    }
                    Auth.AerospikeAuthRequest aerospikeAuthRequest = Auth.AerospikeAuthRequest.newBuilder()
                            .setUsername(clientPolicy.user).setPassword(clientPolicy.password).build();
                    ManagedChannel channel = channelProvider.getChannel();
                    if (channel == null) {
                        // Channel is unavailable. Try again.
                        unsafeScheduleRefresh(10, true);
                        return;
                    }
                    Auth.AerospikeAuthResponse aerospikeAuthResponse =
                            AuthServiceGrpc.newBlockingStub(channel).withDeadline(Deadline.after(refreshMinTime, TimeUnit.MILLISECONDS))
                                    .get(aerospikeAuthRequest);
                    accessToken.token = aerospikeAuthResponse.getToken();
                    // TODO: Should be done in a single call.
                    // TODO: better to create a new token and replace.
                    unsafeSetTokenExpiry();
                    if (Log.debugEnabled()) {
                        Log.debug(String.format("Fetched token successfully " +
                                "with TTL %d", accessToken.ttl.get()));
                    }
                    unsafeScheduleNextRefresh();
                    accessToken.consecutiveRefreshErrors.set(0);
                } catch (Exception e) {
                    accessToken.consecutiveRefreshErrors.incrementAndGet();
                    Log.error(e.getMessage());
                    unsafeScheduleNextRefresh();
                    // TODO: Convert to appropriate Aerospike result
                    throw new AerospikeException(e);
                } finally {
                    tokenFetchLock.unlock();
                }
            }
        }
    }

    private boolean shouldRefresh(boolean forceRefresh) {
        return forceRefresh || isTokenInValid();
    }

    private void unsafeScheduleNextRefresh() {
        long ttl = accessToken.ttl.get();
        long delay = (long) Math.floor(ttl * refreshAfterFraction);

        if (ttl - delay < refreshMinTime) {
            // We need at least refreshMinTimeMillis to refresh, schedule
            // immediately.
            delay = ttl - refreshMinTime;
        }

        if (isTokenInValid()) {
            // Force immediate refresh.
            delay = 0;
        }

        if (delay == 0 && accessToken.consecutiveRefreshErrors.get() > 0) {
            // If we continue to fail then schedule will be too aggressive on fetching new token. Avoid that by increasing
            // fetch delay.

            delay = (long) (Math.pow(2, accessToken.consecutiveRefreshErrors.get()) * 1000);
            if (delay > maxExponentialBackOff) {
                delay = maxExponentialBackOff;
            }

            // Handle wrap around.
            if (delay < 0) {
                delay = 0;
            }
        }
        unsafeScheduleRefresh(delay, true);
    }

    private void unsafeScheduleRefresh(long delay, boolean forceRefresh) {
        tokenFetchLock.lock();
        try {
            if (!forceRefresh || fetchScheduled) {
                return;
            }
            if (!executor.isShutdown()) {
                //noinspection ConstantValue
                accessToken.refreshFuture = executor.schedule(() -> fetchToken(forceRefresh), delay, TimeUnit.MILLISECONDS);
                fetchScheduled = true;
                if (Log.debugEnabled()) {
                    Log.debug(String.format("Scheduled refresh after %d millis", delay));
                }
            }
        } finally {
            tokenFetchLock.unlock();
        }
    }

    private boolean isTokenRequired() {
        return clientPolicy.user != null;
    }

    private boolean isTokenInValid() {
        return isTokenRequired() && (accessToken == null || System.currentTimeMillis() > accessToken.expiry.get());
    }

    private void unsafeSetTokenExpiry() throws IOException {
        String claims = accessToken.token.split("\\.")[1];
        byte[] decodedClaims = Base64.getDecoder().decode(claims);
        @SuppressWarnings("unchecked")
        Map<Object, Object> parsedClaims = objectMapper.readValue(decodedClaims, Map.class);
        Object expiry = parsedClaims.get("exp");
        Object iat = parsedClaims.get("iat");
        if (expiry instanceof Integer && iat instanceof Integer) {
            int ttl = ((Integer) expiry - (Integer) iat) * 1000;
            if (ttl <= 0) {
                throw new IllegalArgumentException("token 'iat' > 'exp'");
            }
            // Set expiry based on local clock.
            accessToken.expiry.set(System.currentTimeMillis() + ttl);
            accessToken.ttl.set(ttl);
        } else {
            throw new IllegalArgumentException("Unsupported access token format");
        }
    }

    @Override
    public void close() {
        // TODO copied from java.util.concurrent.ExecutorService#close available from Java 19.
        boolean terminated = executor.isTerminated();
        if (!terminated) {
            if (accessToken.refreshFuture != null) {
                accessToken.refreshFuture.cancel(true);
            }
            executor.shutdown();
            boolean interrupted = false;
            while (!terminated) {
                try {
                    terminated = executor.awaitTermination(1L, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    if (!interrupted) {
                        executor.shutdownNow();
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }


    private static class AccessToken {
        /**
         * Local token expiry timestamp in millis.
         */
        private final AtomicLong expiry = new AtomicLong(System.currentTimeMillis());
        /**
         * Remaining time to live for the token in millis.
         */
        private final AtomicLong ttl = new AtomicLong(0);
        /**
         * Count of consecutive errors while refreshing the token.
         */
        private final AtomicInteger consecutiveRefreshErrors =
                new AtomicInteger(0);
        /**
         * An access token for Aerospike proxy.
         */
        private volatile String token;
        /**
         * A {@link ScheduledFuture} holding reference to the next auto schedule task.
         */
        private ScheduledFuture<?> refreshFuture;
    }
}
