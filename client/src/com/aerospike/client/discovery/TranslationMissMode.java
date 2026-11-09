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

/**
 * Action taken when an {@link AddressTranslator} has no mapping for an advertised endpoint.
 * See {@link com.aerospike.client.policy.ClientPolicy#translationOnMiss}.
 */
public enum TranslationMissMode {
	/**
	 * Use the advertised endpoint unchanged. YAML value: {@code pass_through}.
	 */
	PASS_THROUGH,

	/**
	 * Do not connect to the advertised endpoint and fail with
	 * {@link com.aerospike.client.AerospikeException.TranslationMiss}. YAML value: {@code strict}.
	 */
	STRICT
}
