/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.proxy.auth.credentials;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import java.util.concurrent.Executor;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;

/**
 * A {@link CallCredentials} implementation to access Aerospike proxy.
 */
public class BearerTokenCallCredentials extends CallCredentials {
	private static final String BEARER_TYPE = "Bearer";
	private static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

	private final String value;

	public BearerTokenCallCredentials(String value) {
		this.value = value;
	}

	@Override
	public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
		executor.execute(() -> {
			try {
				Metadata headers = new Metadata();
				headers.put(AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, value));
				metadataApplier.apply(headers);
			}
			catch (Throwable e) {
				metadataApplier.fail(Status.UNAUTHENTICATED.withCause(e));
			}
		});
	}

	@Override
	public void thisUsesUnstableApi() {
		// noop
	}
}
