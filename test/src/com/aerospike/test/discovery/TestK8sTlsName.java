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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.K8sCluster.Pod;
import com.aerospike.test.util.TestBase;

/**
 * TLS against an Aerospike cluster on the Aerospike Kubernetes Operator, where every pod mounts
 * one shared certificate secret. The certificate names the cluster tls-name, not the endpoint
 * hostname the client dials, so the client must validate each node against the tls-name the
 * cluster advertises.
 */
public class TestK8sTlsName extends TestBase {
	private static final String SET = "l3-tls-name";
	private static final int MIN_PODS = 2;
	private static final int DNS_NAME = 2;
	private static final long ROUTING_WAIT_MS = 120000;
	private static final int KEY_SEARCH_LIMIT = 100000;
	private static final Pattern PEER = Pattern.compile("\\[([0-9A-Fa-f]+),([^,\\[\\]]*),\\[((?:[^\\[\\]]|\\[[^\\]]*\\])*)\\]\\]");
	private static final String TLS_PROFILE = " needs DISCOVERY_TLS_CA_FILE and DISCOVERY_TLS_NAME, set once " +
		".github/discovery/enable-l3-tls.sh serves TLS on the AerospikeCluster (run_l3.sh L3_PROFILE=tls, or the " +
		"discovery-k8s workflow)";

	private K8sCluster scaled;
	private int scaledFrom;

	@Before
	public void requireCluster() {
		assertTrue("Kubernetes TLS name " + K8sCluster.SKIP, ! K8sCluster.kubeconfig.isEmpty());
		assertTrue("Kubernetes TLS name" + TLS_PROFILE,
			! K8sCluster.tlsCaFile.isEmpty() && ! K8sCluster.tlsName.isEmpty());
	}

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		if (scaled != null) {
			K8sCluster cluster = scaled;
			scaled = null;
			cluster.scale(scaledFrom);
		}
	}

	/**
	 * Scaled to at least two pods, every pod advertises the cluster tls-name for each of its peers
	 * and the endpoint hostname with its own Service NodePort as its TLS alternate address. The
	 * shared certificate in the secret carries the tls-name as a dNSName SAN and no SAN for the
	 * endpoint hostname. A client seeded at the endpoint hostname with the tls-name holds one node
	 * per pod, and every partition's master is one of those nodes and serves a write and a read.
	 * The same seed with the endpoint hostname as tls-name fails the TLS handshake.
	 */
	@Test
	public void sharedCertificateCarriesTlsNameAndClientValidatesAgainstIt() throws Exception {
		K8sCluster cluster = K8sCluster.find();
		String endpoint = K8sCluster.endpointHostname;
		String tlsName = K8sCluster.tlsName;
		assertFalse("DISCOVERY_ENDPOINT_HOSTNAME is not set", endpoint.isEmpty());
		assertEquals("AerospikeCluster tls-name", tlsName, cluster.get(".spec.aerospikeConfig.network.service['tls-name']"));
		assertEquals(cluster.get(".spec.aerospikeNetworkPolicy.alternateAccess"),
			cluster.get(".spec.aerospikeNetworkPolicy.tlsAlternateAccess"));

		int size = cluster.size();

		if (size < MIN_PODS) {
			scaled = cluster;
			scaledFrom = size;
			cluster.scale(MIN_PODS);
		}

		assertCertificate(cluster, tlsName, endpoint);

		int tlsPort = Integer.parseInt(cluster.get(".spec.aerospikeConfig.network.service['tls-port']"));
		List<Pod> pods = cluster.pods();
		Map<String,Pod> podsByIp = new HashMap<>();
		Map<String,String> expected = new TreeMap<>();

		for (Pod pod : pods) {
			int nodePort = cluster.nodePort(pod.name, tlsPort);
			System.out.println("L3-TLS-POD " + pod + " tlsServiceNodePort=" + nodePort);
			assertTrue(pod.name + " has no Service NodePort for " + tlsPort, nodePort > 0);
			podsByIp.put(pod.ip, pod);
			expected.put(pod.name, endpoint + ':' + nodePort);
		}

		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		policy.tlsPolicy = new TlsPolicy();
		policy.tlsPolicy.context = trust(K8sCluster.tlsCaFile);
		int seedPort = cluster.nodePort(pods.get(0).name, tlsPort);
		Host seed = new Host(endpoint, tlsName, seedPort);
		captureLog();

		try (AerospikeClient client = new AerospikeClient(policy, seed)) {
			Node[] masters = new Node[Node.PARTITIONS];
			boolean routed = K8sCluster.waitFor(() -> client.getNodes().length == pods.size() &&
				unowned(client, masters) == 0 && owners(masters).size() == pods.size(), ROUTING_WAIT_MS, 500);
			System.out.println("L3-TLS-ROUTING seed=" + seed + " pods=" + pods.size() + " nodes=" +
				client.getNodes().length + " partitions=" + Node.PARTITIONS + " unowned=" + unowned(client, masters) +
				" owners=" + owners(masters));
			assertTrue("Client seeded at " + seed + " did not hold " + pods.size() +
				" nodes owning every partition within " + ROUTING_WAIT_MS + "ms", routed);

			Set<String> nodeNames = new TreeSet<>();
			Set<String> advertisedPeers = new TreeSet<>();
			Set<String> matched = new TreeSet<>();

			for (Node node : client.getNodes()) {
				nodeNames.add(node.getName());
			}

			for (Node node : client.getNodes()) {
				Map<String,String> info = Info.request(null, node, "service-tls-std", "service-tls-alt", "peers-tls-alt");
				Pod pod = podOf(podsByIp, info.get("service-tls-std"));
				assertNotNull(node.getName() + " TLS access " + info.get("service-tls-std") + " is no pod IP in " +
					podsByIp.keySet(), pod);
				assertTrue(pod.name + " matched twice", matched.add(pod.name));
				List<String[]> peers = parsePeers(info.get("peers-tls-alt"));
				System.out.println("L3-TLS-NAME pod=" + pod.name + " node=" + node.getName() + " tlsAlternate=" +
					info.get("service-tls-alt") + " expected=" + expected.get(pod.name) + " clientTlsName=" +
					node.getHost().tlsName + " peersTlsAlt=" + info.get("peers-tls-alt"));
				assertEquals(pod.name + " TLS alternate address", expected.get(pod.name), info.get("service-tls-alt"));
				assertEquals(node.getName() + " client tls-name", tlsName, node.getHost().tlsName);
				assertEquals(pod.name + " peers " + info.get("peers-tls-alt"), pods.size() - 1, peers.size());

				for (String[] peer : peers) {
					assertEquals(pod.name + " tls-name for peer " + peer[0], tlsName, peer[1]);
					assertTrue(pod.name + " advertises unknown peer " + peer[0] + ", nodes " + nodeNames,
						nodeNames.contains(peer[0]));
					advertisedPeers.add(peer[0]);
				}
			}
			assertEquals(expected.keySet(), matched);
			assertEquals("Nodes advertised as peers", nodeNames, advertisedPeers);
			assertServedByEveryNode(client, masters);
		}

		Host fqdn = new Host(endpoint, endpoint, seedPort);

		try (AerospikeClient client = new AerospikeClient(policy, fqdn)) {
			fail("TLS handshake with " + fqdn + " accepted a certificate that names only " + tlsName + "; nodes=" +
				List.of(client.getNodeNames()));
		}
		catch (AerospikeException ae) {
			String message = messages(ae);
			System.out.println("L3-TLS-FQDN-REJECTED seed=" + fqdn + " error=" + message.replaceAll("\\s*\n\\s*", " "));
			assertTrue(message, message.contains("Invalid TLS name: " + endpoint));
		}
	}

	/**
	 * Read the server certificate from the secret mounted where the tls-name's cert-file lives and
	 * assert its dNSName SANs are exactly the tls-name, with no SAN of any type for the endpoint.
	 */
	private static void assertCertificate(K8sCluster cluster, String tlsName, String endpoint) throws Exception {
		String certFile = cluster.get(".spec.aerospikeConfig.network.tls[?(@.name==\"" + tlsName + "\")]['cert-file']");
		int index = certFile.lastIndexOf('/');
		assertTrue("cert-file " + certFile, index > 0);
		String dir = certFile.substring(0, index);
		String secret = cluster.get(".spec.storage.volumes[?(@.aerospike.path==\"" + dir + "\")].source.secret.secretName");
		assertFalse("No secret volume at " + dir, secret.isEmpty());

		X509Certificate cert;

		try (InputStream in = new ByteArrayInputStream(cluster.secretData(secret, certFile.substring(index + 1)))) {
			cert = (X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(in);
		}

		List<String> dnsNames = new ArrayList<>();
		List<String> sans = new ArrayList<>();
		Collection<List<?>> entries = cert.getSubjectAlternativeNames();

		if (entries != null) {
			for (List<?> entry : entries) {
				sans.add(entry.get(0) + ":" + entry.get(1));

				if (((Integer)entry.get(0)) == DNS_NAME) {
					dnsNames.add((String)entry.get(1));
				}
			}
		}
		System.out.println("L3-TLS-CERT secret=" + cluster.namespace + '/' + secret + " key=" +
			certFile.substring(index + 1) + " subject=" + cert.getSubjectX500Principal() + " sans=" + sans +
			" dnsNames=" + dnsNames + " endpoint=" + endpoint);
		assertEquals("dNSName SANs", List.of(tlsName), dnsNames);

		for (List<?> entry : entries) {
			assertFalse("SAN " + entry + " names the endpoint " + endpoint,
				endpoint.equalsIgnoreCase(String.valueOf(entry.get(1))));
		}
	}

	/**
	 * Write and read one key per node with replica MASTER, each key in a partition the node is
	 * master of, and assert the client sent both commands to that node.
	 */
	private static void assertServedByEveryNode(AerospikeClient client, Node[] masters) {
		WritePolicy wp = client.copyWritePolicyDefault();
		wp.replica = Replica.MASTER;
		Policy rp = client.copyReadPolicyDefault();
		rp.replica = Replica.MASTER;

		for (Node node : client.getNodes()) {
			Key key = null;

			for (int i = 0; i < KEY_SEARCH_LIMIT && key == null; i++) {
				Key candidate = new Key(args.namespace, SET, "tls-" + i);

				if (masters[Partition.getPartitionId(candidate.digest)] == node) {
					key = candidate;
				}
			}
			assertNotNull(node.getName() + " is master of no partition", key);
			assertSame(node, Partition.write(client.getCluster(), wp, key).getNodeWrite(client.getCluster()));
			client.put(wp, key, new Bin("v", node.getName()));
			assertEquals(node.getName(), client.get(rp, key).getString("v"));
			System.out.println("L3-TLS-SERVED node=" + node.getName() + " key=" + key.userKey + " partition=" +
				Partition.getPartitionId(key.digest) + " write=ok read=ok");
		}
	}

	private static SSLContext trust(String caFile) throws Exception {
		KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
		store.load(null, null);

		try (InputStream in = Files.newInputStream(Paths.get(caFile))) {
			store.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
		}

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(store);
		SSLContext context = SSLContext.getInstance("TLS");
		context.init(null, tmf.getTrustManagers(), null);
		return context;
	}

	/**
	 * Parse a peers-tls reply, {@code gen,port,[[node,tls-name,[address[:port],...]],...]}, into
	 * {node, tls-name} pairs.
	 */
	private static List<String[]> parsePeers(String reply) {
		List<String[]> peers = new ArrayList<>();

		if (reply == null) {
			return peers;
		}

		Matcher m = PEER.matcher(reply);

		while (m.find()) {
			peers.add(new String[] {m.group(1), m.group(2)});
		}
		return peers;
	}

	private static int unowned(AerospikeClient client, Node[] masters) {
		Partitions partitions = client.getCluster().partitionMap.get(args.namespace);
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = partitions == null ? null : partitions.replicas[0].get(i);

			if (node == null || ! node.isActive() || client.getCluster().getNode(node.getName()) != node) {
				masters[i] = null;
				unowned++;
			}
			else {
				masters[i] = node;
			}
		}
		return unowned;
	}

	private static Map<String,Integer> owners(Node[] masters) {
		Map<String,Integer> owners = new TreeMap<>();

		for (Node node : masters) {
			if (node != null) {
				owners.merge(node.getName(), 1, Integer::sum);
			}
		}
		return owners;
	}

	private static Pod podOf(Map<String,Pod> podsByIp, String access) {
		if (access == null) {
			return null;
		}

		for (Host h : Host.parseServiceHosts(access)) {
			Pod pod = podsByIp.get(h.name);

			if (pod != null) {
				return pod;
			}
		}
		return null;
	}

	private static String messages(Throwable t) {
		StringBuilder sb = new StringBuilder();

		for (Throwable e = t; e != null; e = e.getCause()) {
			if (sb.length() > 0) {
				sb.append(" <- ");
			}
			sb.append(e.getMessage());
		}
		return sb.toString();
	}

	private static void captureLog() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> System.out.println("CLIENT-LOG " + level + ' ' + message));
	}
}
