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
package com.aerospike.test.tls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

import io.netty.channel.nio.NioEventLoopGroup;

/**
 * Server certificate name matching against a TLS-enabled server whose certificate shape is
 * chosen per run by test/tls/run_tls_matching.sh. The case arrives as system properties:
 * <ul>
 * <li>tls.case: case name</li>
 * <li>tls.name: tlsName used as the reference identifier</li>
 * <li>tls.expect: accept or reject</li>
 * <li>tls.warning: true if the legacy-match deprecation warning must be logged</li>
 * <li>tls.bootstrap: tlsName the certificate accepts, used to reach the netty path on reject</li>
 * <li>tls.revoke: optional hex serial placed in {@link TlsPolicy#revokeCertificates}</li>
 * <li>tls.ca, tls.host, tls.port, tls.namespace: CA file and server address</li>
 * </ul>
 */
public class TestTlsMatching {
	private static final String TLS_NAME_PREFIX = "Invalid TLS name: ";
	private static final String SERIAL_PREFIX = "Invalid certificate serial number: ";
	private static final int TIMEOUT_MS = 5000;
	private static final int ASYNC_WAIT_SECONDS = 10;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private static String caseName;
	private static String tlsName;
	private static boolean expectAccept;
	private static boolean expectWarning;
	private static String bootstrapName;
	private static BigInteger revoke;
	private static String hostName;
	private static int port;
	private static String namespace;
	private static SSLContext sslContext;
	private static NioEventLoopGroup group;
	private static NettyEventLoops eventLoops;

	@BeforeClass
	public static void init() throws Exception {
		caseName = required("tls.case");
		tlsName = required("tls.name");
		expectAccept = required("tls.expect").equals("accept");
		expectWarning = Boolean.parseBoolean(required("tls.warning"));
		bootstrapName = System.getProperty("tls.bootstrap", "");
		String rv = System.getProperty("tls.revoke", "");
		revoke = rv.isEmpty() ? null : new BigInteger(rv, 16);
		hostName = System.getProperty("tls.host", "127.0.0.1");
		port = Integer.parseInt(required("tls.port"));
		namespace = System.getProperty("tls.namespace", "test");
		sslContext = trustContext(required("tls.ca"));

		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
			System.out.println("CLIENT-LOG " + level + ' ' + message);
		});

		group = new NioEventLoopGroup(1);
		eventLoops = new NettyEventLoops(new EventPolicy(), group, EventLoopType.NETTY_NIO);
	}

	@AfterClass
	public static void destroy() {
		if (eventLoops != null) {
			eventLoops.close();
		}
		Log.setCallback(null);
	}

	@Test
	public void matching() throws Exception {
		Outcome sync = runSync();
		long warnFirst = warningCount();
		Outcome async = runAsync();
		long warnSecond = warningCount();

		System.out.println("TLS-RESULT case=" + caseName + " tlsName=" + tlsName +
			" expected=" + (expectAccept ? "accept" : "reject") +
			" sync=" + sync + " async=" + async +
			" warningExpected=" + expectWarning +
			" warningsAfterFirstClient=" + warnFirst +
			" warningsAfterSecondClient=" + warnSecond);

		String prefix = (revoke != null) ? SERIAL_PREFIX : TLS_NAME_PREFIX;

		assertOutcome("sync", sync, prefix);
		assertOutcome("async", async, prefix);
		assertEquals("warnings after first client", expectWarning ? 1 : 0, warnFirst);
		assertEquals("warnings after second client", expectWarning ? 1 : 0, warnSecond);
	}

	private static void assertOutcome(String path, Outcome outcome, String prefix) {
		if (expectAccept) {
			assertTrue(path + " expected accept: " + outcome, outcome.accepted);
			return;
		}
		assertFalse(path + " expected reject", outcome.accepted);
		assertNotNull(path + " exception", outcome.error);
		assertFalse(path + " must not be retryable: " + outcome, outcome.error instanceof AerospikeException.Connection);
		assertTrue(path + " message: " + outcome, outcome.error.getBaseMessage().startsWith(prefix));

		if (outcome.clientError != null) {
			assertTrue(path + " client message: " + outcome.clientError.getMessage(),
				outcome.clientError.getMessage().contains(prefix));
		}
	}

	private Outcome runSync() {
		Outcome outcome = new Outcome();
		TlsPolicy tp = tlsPolicy();
		tp.revokeCertificates = (revoke != null) ? new BigInteger[] {revoke} : null;

		try {
			Connection conn = new Connection(tp, tlsName, new InetSocketAddress(hostName, port), TIMEOUT_MS);
			conn.close();
		}
		catch (AerospikeException ae) {
			outcome.error = ae;
		}

		ClientPolicy cp = clientPolicy(tp);

		try (AerospikeClient client = new AerospikeClient(cp, new Host(hostName, tlsName, port))) {
			Key key = new Key(namespace, "tls", caseName);
			client.put(null, key, new Bin("v", caseName));
			Record rec = client.get(null, key);
			outcome.accepted = outcome.error == null && rec != null && caseName.equals(rec.getString("v"));
		}
		catch (AerospikeException ae) {
			outcome.clientError = ae;
		}
		return outcome;
	}

	private Outcome runAsync() throws Exception {
		Outcome outcome = new Outcome();
		TlsPolicy tp = tlsPolicy();
		ClientPolicy cp = clientPolicy(tp);
		cp.eventLoops = eventLoops;

		AerospikeClient client;

		try {
			client = new AerospikeClient(cp, new Host(hostName, tlsName, port));
		}
		catch (AerospikeException ae) {
			if (expectAccept || bootstrapName.isEmpty()) {
				outcome.clientError = ae;
				return outcome;
			}
			// Sync tend rejected the name. Reach the netty path through a name the certificate
			// accepts, then present the case name to netty.
			client = new AerospikeClient(cp, new Host(hostName, bootstrapName, port));
			outcome.bootstrap = bootstrapName;
			Node node = client.getNodes()[0];
			Field f = Node.class.getDeclaredField("host");
			f.setAccessible(true);
			Host h = node.getHost();
			f.set(node, new Host(h.name, tlsName, h.port));
		}

		try {
			if (revoke != null) {
				tp.revokeCertificates = new BigInteger[] {revoke};
			}

			EventLoop loop = eventLoops.next();
			Key key = new Key(namespace, "tls", caseName + "-async");
			CompletableFuture<Void> put = new CompletableFuture<>();

			client.put(loop, new WriteListener() {
				public void onSuccess(Key k) {
					put.complete(null);
				}
				public void onFailure(AerospikeException e) {
					put.completeExceptionally(e);
				}
			}, null, key, new Bin("v", caseName));

			try {
				put.get(ASYNC_WAIT_SECONDS, TimeUnit.SECONDS);
			}
			catch (java.util.concurrent.ExecutionException ee) {
				outcome.error = (AerospikeException)ee.getCause();
				return outcome;
			}

			CompletableFuture<Record> get = new CompletableFuture<>();

			client.get(loop, new RecordListener() {
				public void onSuccess(Key k, Record r) {
					get.complete(r);
				}
				public void onFailure(AerospikeException e) {
					get.completeExceptionally(e);
				}
			}, null, key);

			Record rec = get.get(ASYNC_WAIT_SECONDS, TimeUnit.SECONDS);
			outcome.accepted = rec != null && caseName.equals(rec.getString("v"));
			return outcome;
		}
		finally {
			client.close();
		}
	}

	private static TlsPolicy tlsPolicy() {
		TlsPolicy tp = new TlsPolicy();
		tp.context = sslContext;
		return tp;
	}

	private static ClientPolicy clientPolicy(TlsPolicy tp) {
		ClientPolicy cp = new ClientPolicy();
		cp.tlsPolicy = tp;
		cp.timeout = TIMEOUT_MS;
		cp.failIfNotConnected = true;
		return cp;
	}

	private static long warningCount() {
		String quoted = "'" + tlsName + "'";
		return warnings.stream().filter(m -> m.contains(quoted)).count();
	}

	private static SSLContext trustContext(String caFile) throws Exception {
		X509Certificate ca;

		try (InputStream is = new FileInputStream(caFile)) {
			ca = (X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(is);
		}

		KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
		ks.load(null, null);
		ks.setCertificateEntry("ca", ca);

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(ks);

		SSLContext ctx = SSLContext.getInstance("TLS");
		ctx.init(null, tmf.getTrustManagers(), null);
		return ctx;
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is required. Run test/tls/run_tls_matching.sh");
		}
		return value;
	}

	private static final class Outcome {
		boolean accepted;
		AerospikeException error;
		AerospikeException clientError;
		String bootstrap;

		@Override
		public String toString() {
			String via = (bootstrap != null) ? "(netty,bootstrap=" + bootstrap + ")" : "";

			if (accepted) {
				return "accept" + via;
			}
			AerospikeException e = (error != null) ? error : clientError;
			return "reject[" + (e == null ? "no-exception" : e.getClass().getSimpleName() + ": " +
				e.getBaseMessage().replace('\n', ' ').replace(' ', '_')) + "]" + via;
		}
	}
}
