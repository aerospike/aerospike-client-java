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

import com.aerospike.client.Host;

/**
 * Rewrite of a server advertised address into the address the client connects to. The client
 * applies the translator to every address returned by peers and service info commands.
 * <p>
 * When no translator is configured, {@link StaticMapAddressTranslator} is used with
 * {@link com.aerospike.client.policy.ClientPolicy#ipMap}.
 */
public interface AddressTranslator {
	/**
	 * Return the host the client should connect to for the advertised endpoint. Return a host
	 * with the advertised name and port to leave the address unchanged.
	 * <p>
	 * This method is called on the cluster tend thread and must be in-memory only. It must not
	 * perform I/O of any kind, including DNS resolution.
	 * <p>
	 * The TLS name is never rewritten. The client always uses the advertised endpoint's TLS name,
	 * regardless of the TLS name on the returned host.
	 */
	Host translate(Endpoint advertised);
}
