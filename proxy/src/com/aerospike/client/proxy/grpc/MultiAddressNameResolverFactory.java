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
package com.aerospike.client.proxy.grpc;

import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;

class MultiAddressNameResolverFactory extends NameResolver.Factory {

	final List<EquivalentAddressGroup> addresses;

	MultiAddressNameResolverFactory(List<SocketAddress> addresses) {
		this.addresses = addresses.stream()
				.map(EquivalentAddressGroup::new)
				.collect(Collectors.toList());
	}

	public NameResolver newNameResolver(URI notUsedUri, NameResolver.Args args) {
		return new NameResolver() {
			@Override
			public String getServiceAuthority() {
				return "Authority";
			}

			public void start(Listener2 listener) {
				listener.onResult(ResolutionResult.newBuilder().setAddresses(addresses)
						.setAttributes(Attributes.EMPTY).build());
			}

			public void shutdown() {
			}
		};
	}

	@Override
	public String getDefaultScheme() {
		return "multiaddress";
	}
}