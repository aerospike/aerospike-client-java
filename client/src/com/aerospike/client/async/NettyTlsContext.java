/*
 * Copyright 2012-2022 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

/**
 * Netty SslContext container.
 */
public final class NettyTlsContext implements CipherSuiteFilter {
	private final TlsPolicy policy;
	private final SslContext context;

	/**
	 * Construct Netty SslContext.
	 */
	public NettyTlsContext(TlsPolicy policy) {
		this.policy = policy;

		if (policy.context != null) {
			CipherSuiteFilter csf = (policy.ciphers != null)? this : IdentityCipherSuiteFilter.INSTANCE;
			this.context = new JdkSslContext(policy.context, true, null, csf, null, ClientAuth.NONE, null, false);
			return;
		}

		try {
			SslContextBuilder builder = SslContextBuilder.forClient();

			if (policy.protocols != null) {
				builder.protocols(policy.protocols);
			}

			if (policy.ciphers != null) {
				builder.ciphers(Arrays.asList(policy.ciphers));
			}

			String keyStoreLocation = System.getProperty("javax.net.ssl.keyStore");

			// Keystore is only required for mutual authentication.
			if (keyStoreLocation != null) {
				String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
				char[] pass = (keyStorePassword != null) ? keyStorePassword.toCharArray() : null;

				KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

				try (FileInputStream is = new FileInputStream(keyStoreLocation)) {
					ks.load(is, pass);
				}

				KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				kmf.init(ks, pass);

				builder.keyManager(kmf);
			}

			this.context = builder.build();
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to init netty TLS: " + Util.getErrorMessage(e));
		}
	}

	/**
	 * Create TLS handler.
	 */
	public SslHandler createHandler(SocketChannel ch) {
		return context.newHandler(ch.alloc());
	}

	/**
	 * Return supported ciphers.
	 */
	@Override
	public String[] filterCipherSuites(Iterable<String> ciphers, List<String> defaultCiphers, Set<String> supportedCiphers) {
		if (policy.ciphers != null) {
			return policy.ciphers;
		}
		return policy.context.getSupportedSSLParameters().getCipherSuites();
	}
}
