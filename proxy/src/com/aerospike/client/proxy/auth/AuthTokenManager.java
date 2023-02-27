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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import io.grpc.stub.StreamObserver;

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
	private final AtomicBoolean isFetchingToken = new AtomicBoolean(false);
	private final AtomicBoolean isClosed = new AtomicBoolean(false);
	/**
	 * Count of consecutive errors while refreshing the token.
	 */
	private final AtomicInteger consecutiveRefreshErrors =
			new AtomicInteger(0);
	private volatile AccessToken accessToken;
	private volatile boolean fetchScheduled;
	/**
	 * A {@link ScheduledFuture} holding reference to the next auto schedule task.
	 */
	private ScheduledFuture<?> refreshFuture;

	public AuthTokenManager(ClientPolicy clientPolicy, GrpcChannelProvider grpcCallExecutor) {
		this.clientPolicy = clientPolicy;
		this.channelProvider = grpcCallExecutor;
		this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("auth-manager").build());
		this.accessToken = new AccessToken(System.currentTimeMillis(), 0, "");
		fetchToken(true);
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
		if (isClosed.get() || !isTokenRequired() || isFetchingToken.get()) {
			return;
		}
		if (shouldRefresh(forceRefresh)) {
			try {
				if (Log.debugEnabled()) {
					Log.debug("Starting token refresh");
				}
				Auth.AerospikeAuthRequest aerospikeAuthRequest = Auth.AerospikeAuthRequest.newBuilder()
						.setUsername(clientPolicy.user).setPassword(clientPolicy.password).build();
				ManagedChannel channel = channelProvider.getControlChannel();
				if (channel == null) {
					isFetchingToken.set(false);
					// Channel is unavailable. Try again.
					unsafeScheduleRefresh(10, true);
					return;
				}

				isFetchingToken.set(true);
				AuthServiceGrpc.newStub(channel).withDeadline(Deadline.after(refreshMinTime, TimeUnit.MILLISECONDS))
						.get(aerospikeAuthRequest, new StreamObserver<Auth.AerospikeAuthResponse>() {
							@Override
							public void onNext(Auth.AerospikeAuthResponse aerospikeAuthResponse) {
								try {
									accessToken =
											parseToken(aerospikeAuthResponse.getToken());
									if (Log.debugEnabled()) {
										Log.debug(String.format("Fetched token successfully " +
												"with TTL %d", accessToken.ttl));
									}
									unsafeScheduleNextRefresh();
									consecutiveRefreshErrors.set(0);
								}
								catch (Exception e) {
									onFetchError(e);
								}
							}

							@Override
							public void onError(Throwable t) {
								onFetchError(t);
							}

							@Override
							public void onCompleted() {
								isFetchingToken.set(false);
							}
						});

			}
			catch (Exception e) {
				onFetchError(e);
			}
		}
	}

	private void onFetchError(Throwable t) {
		consecutiveRefreshErrors.incrementAndGet();
		Log.error(t.getMessage());
		unsafeScheduleNextRefresh();
		isFetchingToken.set(false);
	}

	private boolean shouldRefresh(boolean forceRefresh) {
		return forceRefresh || !isTokenValid();
	}

	private void unsafeScheduleNextRefresh() {
		long ttl = accessToken.ttl;
		long delay = (long)Math.floor(ttl * refreshAfterFraction);

		if (ttl - delay < refreshMinTime) {
			// We need at least refreshMinTimeMillis to refresh, schedule
			// immediately.
			delay = ttl - refreshMinTime;
		}

		if (!isTokenValid()) {
			// Force immediate refresh.
			delay = 0;
		}

		if (delay == 0 && consecutiveRefreshErrors.get() > 0) {
			// If we continue to fail then schedule will be too aggressive on fetching new token. Avoid that by increasing
			// fetch delay.

			delay = (long)(Math.pow(2, consecutiveRefreshErrors.get()) * 1000);
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
		if (isClosed.get() || !forceRefresh || fetchScheduled) {
			return;
		}
		if (!executor.isShutdown()) {
			//noinspection ConstantValue
			refreshFuture = executor.schedule(() -> fetchToken(forceRefresh), delay, TimeUnit.MILLISECONDS);
			fetchScheduled = true;
			if (Log.debugEnabled()) {
				Log.debug(String.format("Scheduled refresh after %d millis", delay));
			}
		}
	}

	private boolean isTokenRequired() {
		return clientPolicy.user != null;
	}

	private AccessToken parseToken(String token) throws IOException {
		String claims = token.split("\\.")[1];
		byte[] decodedClaims = Base64.getDecoder().decode(claims);
		@SuppressWarnings("unchecked")
		Map<Object, Object> parsedClaims = objectMapper.readValue(decodedClaims, Map.class);
		Object expiryToken = parsedClaims.get("exp");
		Object iat = parsedClaims.get("iat");
		if (expiryToken instanceof Integer && iat instanceof Integer) {
			int ttl = ((Integer)expiryToken - (Integer)iat) * 1000;
			if (ttl <= 0) {
				throw new IllegalArgumentException("token 'iat' > 'exp'");
			}
			// Set expiry based on local clock.
			long expiry = System.currentTimeMillis() + ttl;
			return new AccessToken(expiry, ttl, token);
		}
		else {
			throw new IllegalArgumentException("Unsupported access token format");
		}
	}

	public CallOptions setCallCredentials(
			CallOptions callOptions) {
		if (isTokenRequired()) {
			if (!isTokenValid()) {
				if (Log.warnEnabled()) {
					// TODO: This warns for evey call, spamming the output.
					//  Should be rate limited. Possibly once in a few seconds.
					// This alerts that auto refresh didn't finish correctly. In normal scenario, this should never
					// happen.
					Log.warn("Trying to refresh token before setting into call");
				}
				unsafeScheduleRefresh(0, false);
			}
			if (!isTokenValid()) {
				throw new IllegalStateException("Access token has expired");
			}
			return callOptions.withCallCredentials(new BearerTokenCallCredentials(accessToken.token));
		}
		return callOptions;
	}

	/**
	 * @return the minimum amount of time it takes for the token to refresh.
	 */
	public int getRefreshMinTime() {
		return refreshMinTime;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean isTokenValid() {
		AccessToken token = accessToken;
		return !isTokenRequired() || (token != null && System.currentTimeMillis() <= token.expiry);
	}

	@Override
	public void close() {
		if (isClosed.get()) {
			return;
		}

		isClosed.set(true);
		// TODO copied from java.util.concurrent.ExecutorService#close available from Java 19.
		boolean terminated = executor.isTerminated();
		if (!terminated) {
			if (refreshFuture != null) {
				refreshFuture.cancel(true);
			}
			executor.shutdown();
			boolean interrupted = false;
			while (!terminated) {
				try {
					terminated = executor.awaitTermination(1L, TimeUnit.DAYS);
				}
				catch (InterruptedException e) {
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
		private final long expiry;
		/**
		 * Remaining time to live for the token in millis.
		 */
		private final long ttl;
		/**
		 * An access token for Aerospike proxy.
		 */
		private final String token;

		public AccessToken(long expiry, long ttl, String token) {
			this.expiry = expiry;
			this.ttl = ttl;
			this.token = token;
		}
	}
}
