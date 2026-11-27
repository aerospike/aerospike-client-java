/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

import com.aerospike.client.AerospikeException;

/**
 * Exception thrown by {@link SeedCandidateProvider#refreshSeedCandidates(SeedCandidateContext)} when
 * discovery fails irrecoverably, such as on a credential or API failure. Unlike an empty result, it
 * carries the reason. The client handles it as any other exception from the provider: at client
 * initialization it is logged at warning level and construction fails with
 * {@link com.aerospike.client.ResultCode#SEED_DISCOVERY_FAILED} when no seed host is left; on a
 * periodic refresh it is logged at warning level and the previous seed list is kept.
 */
public class SeedCandidateException extends AerospikeException {
	private static final long serialVersionUID = 1L;

	public SeedCandidateException(String message) {
		super(message);
	}

	public SeedCandidateException(String message, Throwable e) {
		super(message, e);
	}
}
