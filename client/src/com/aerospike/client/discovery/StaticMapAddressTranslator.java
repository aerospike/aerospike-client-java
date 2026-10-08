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

import java.util.Map;

import com.aerospike.client.Host;

/**
 * Address translator that replaces the advertised host name with the value mapped to it.
 * The port is kept. The advertised address is returned unchanged when the host name is not
 * mapped or the map is null.
 */
public final class StaticMapAddressTranslator implements AddressTranslator {
	private final Map<String,String> ipMap;

	/**
	 * Initialize translator with a map of advertised host name to the host name to connect to.
	 * The map is referenced, not copied. It may be null.
	 */
	public StaticMapAddressTranslator(Map<String,String> ipMap) {
		this.ipMap = ipMap;
	}

	/**
	 * Return the advertised endpoint with its host name replaced when mapped. In-memory lookup only.
	 */
	@Override
	public Host translate(Endpoint advertised) {
		String host = advertised.host;

		if (ipMap != null) {
			String alternativeHost = ipMap.get(host);

			if (alternativeHost != null) {
				host = alternativeHost;
			}
		}
		return new Host(host, advertised.tlsName, advertised.port);
	}
}
