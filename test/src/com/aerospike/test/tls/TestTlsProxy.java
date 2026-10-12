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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

/**
 * Testing-strategy N-6: TLS through a TCP pass-through listener. A two node cluster is reached
 * only through one local forwarder per node. The second node is never seeded: the client learns
 * it, and its tls-name, from peers-* and reaches it through an address translator that also
 * tries to set the TLS name to the endpoint address. The certificate carries the tls-name only
 * as a dNSName SAN with a non-matching CN, and no iPAddress SAN, so the handshake succeeds only
 * when it validates against the node's tls-name and not the endpoint or the CN.
 * <p>
 * Run by test/tls/run_tls_matching.sh (case n6_proxy), which supplies:
 * <ul>
 * <li>tls.name: tls-name configured on both nodes</li>
 * <li>tls.nodes: node name to published TLS port, e.g. A1=4433,B2=4434. The first is the seed.</li>
 * <li>tls.ca, tls.host, tls.namespace: CA file, published address and namespace</li>
 * </ul>
 */
public class TestTlsProxy {
	private static final int TIMEOUT_MS = 5000;
	private static final int KEY_COUNT = 50;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private static String tlsName;
	private static String hostName;
	private static String namespace;
	private static String seedNode;
	private static final Map<String,Forwarder> forwarders = new HashMap<>();
	private static SSLContext sslContext;

	@BeforeClass
	public static void init() throws Exception {
		tlsName = required("tls.name");
		hostName = System.getProperty("tls.host", "127.0.0.1");
		namespace = System.getProperty("tls.namespace", "test");
		sslContext = trustContext(required("tls.ca"));

		for (String entry : required("tls.nodes").split(",")) {
			String[] kv = entry.split("=");

			if (seedNode == null) {
				seedNode = kv[0];
			}
			forwarders.put(kv[0], new Forwarder(hostName, Integer.parseInt(kv[1])));
		}

		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
			System.out.println("CLIENT-LOG " + level + ' ' + message);
		});
	}

	@AfterClass
	public static void destroy() {
		for (Forwarder f : forwarders.values()) {
			f.close();
		}
		Log.setCallback(null);
	}

	@Test
	public void throughPassThroughListener() {
		TlsPolicy tp = new TlsPolicy();
		tp.context = sslContext;

		ClientPolicy cp = new ClientPolicy();
		cp.tlsPolicy = tp;
		cp.timeout = TIMEOUT_MS;
		cp.failIfNotConnected = true;
		cp.addressTranslator = advertised -> {
			Forwarder f = forwarders.get(advertised.nodeName);
			int port = (f != null) ? f.port() : advertised.port;
			return new Host(f != null ? Forwarder.ADDRESS : advertised.host, Forwarder.ADDRESS, port);
		};

		Host seed = new Host(Forwarder.ADDRESS, tlsName, forwarders.get(seedNode).port());

		try (AerospikeClient client = new AerospikeClient(cp, seed)) {
			assertTrue("cluster did not discover both nodes", waitForNodes(client, forwarders.size()));

			for (Node node : client.getNodes()) {
				Host h = node.getHost();
				Forwarder f = forwarders.get(node.getName());
				System.out.println("TLS-PROXY node=" + node.getName() + " host=" + h + " tlsName=" + h.tlsName +
					" forwarderConnections=" + (f != null ? f.accepted() : -1));

				assertNotNull("unexpected node " + node.getName(), f);
				assertEquals("node " + node.getName() + " endpoint", Forwarder.ADDRESS, h.name);
				assertEquals("node " + node.getName() + " port", f.port(), h.port);
				assertEquals("node " + node.getName() + " tlsName", tlsName, h.tlsName);
				assertEquals("node " + node.getName() + " info", node.getName(), Info.request(node, "node"));
				assertTrue("no connection through forwarder for " + node.getName(), f.accepted() > 0);
			}

			for (int i = 0; i < KEY_COUNT; i++) {
				Key key = new Key(namespace, "tls", "proxy-" + i);
				client.put(null, key, new Bin("v", i));
				Record rec = client.get(null, key);
				assertNotNull("record " + i, rec);
				assertEquals("record " + i, i, rec.getInt("v"));
			}
		}

		String quoted = "'" + tlsName + "'";
		assertEquals("legacy match warnings", 0, warnings.stream().filter(m -> m.contains(quoted)).count());
	}

	private static boolean waitForNodes(AerospikeClient client, int count) {
		long deadline = System.currentTimeMillis() + TIMEOUT_MS;

		while (System.currentTimeMillis() < deadline) {
			if (client.getNodes().length == count) {
				return true;
			}
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return client.getNodes().length == count;
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
			throw new IllegalStateException("System property " + name + " is required. Run test/tls/run_tls_matching.sh n6_proxy");
		}
		return value;
	}

	/**
	 * TCP pass-through listener on an ephemeral loopback port. Bytes are copied unchanged in both
	 * directions, so the node terminates TLS.
	 */
	private static final class Forwarder implements Closeable {
		static final String ADDRESS = "127.0.0.1";

		private final ServerSocket server;
		private final String targetHost;
		private final int targetPort;
		private final AtomicInteger accepted = new AtomicInteger();
		private final List<Socket> sockets = new CopyOnWriteArrayList<>();

		Forwarder(String targetHost, int targetPort) throws IOException {
			this.server = new ServerSocket(0, 50, InetAddress.getByName(ADDRESS));
			this.targetHost = targetHost;
			this.targetPort = targetPort;

			Thread t = new Thread(this::acceptLoop, "forwarder-" + targetPort);
			t.setDaemon(true);
			t.start();
		}

		int port() {
			return server.getLocalPort();
		}

		int accepted() {
			return accepted.get();
		}

		private void acceptLoop() {
			while (!server.isClosed()) {
				try {
					Socket client = server.accept();
					Socket target = new Socket(targetHost, targetPort);
					sockets.add(client);
					sockets.add(target);
					accepted.incrementAndGet();
					pump(client, target);
					pump(target, client);
				}
				catch (IOException e) {
					// Closed or target unreachable.
				}
			}
		}

		private static void pump(Socket from, Socket to) {
			Thread t = new Thread(() -> {
				byte[] buf = new byte[8192];

				try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
					int n;

					while ((n = in.read(buf)) >= 0) {
						out.write(buf, 0, n);
						out.flush();
					}
				}
				catch (IOException e) {
					// Peer closed.
				}
				finally {
					closeQuietly(from);
					closeQuietly(to);
				}
			}, "forwarder-pump");
			t.setDaemon(true);
			t.start();
		}

		@Override
		public void close() {
			closeQuietly(server);

			for (Socket s : sockets) {
				closeQuietly(s);
			}
		}

		private static void closeQuietly(Closeable c) {
			try {
				c.close();
			}
			catch (IOException e) {
				// Ignore.
			}
		}
	}
}
