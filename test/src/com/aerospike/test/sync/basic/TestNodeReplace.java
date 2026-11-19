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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Peer;
import com.aerospike.client.cluster.Peers;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.util.TestBase;

/**
 * CLIENT-3117: Replace an existing node in the cluster when a new peer has the same node name,
 * but a different IP address. The existing node must also have recent cluster tend failures
 * and not be a localhost IP address for the node replacement to proceed.
 * <p>
 * Exercises the peer/node matching decision in Node.findPeerNode() against a real cluster node.
 * A dedicated client with a very long tend interval is used so the background tend thread does
 * not reset node state (failures, address) while each test manipulates it.
 */
public class TestNodeReplace extends TestBase {
	private static AerospikeClient client;
	private static Cluster cluster;
	private static Node node;
	private static InetSocketAddress origAddress;
	private static InetSocketAddress remoteAddress;
	private static InetSocketAddress localAddress;
	private static Host otherHost;

	private static Method findPeerNode;
	private static Field nodeFailures;
	private static Field nodeReferenceCount;
	private static Field nodeAddress;
	private static Field peerNodeName;
	private static Field peerHosts;
	private static Field peerReplaceNode;

	@BeforeClass
	public static void init() throws Exception {
		assumeFalse("Node replacement does not apply to proxy client", args.useProxyClient);

		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		// Effectively disable background tending after the initial tend.
		policy.tendInterval = 3600 * 1000;

		Host[] hosts = Host.parseHosts(args.host, args.port);
		client = new AerospikeClient(policy, hosts);
		cluster = client.getCluster();
		node = cluster.getNodes()[0];
		origAddress = node.getAddress();

		int port = origAddress.getPort();

		// Use TEST-NET-1 (RFC 5737) addresses that will never match the real node.
		remoteAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[] {(byte)192, 0, 2, 1}), port);
		localAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), port);
		otherHost = new Host("192.0.2.2", port);

		findPeerNode = Node.class.getDeclaredMethod("findPeerNode", Cluster.class, Peers.class, Peer.class);
		findPeerNode.setAccessible(true);

		nodeFailures = accessible(Node.class, "failures");
		nodeReferenceCount = accessible(Node.class, "referenceCount");
		nodeAddress = accessible(Node.class, "address");
		peerNodeName = accessible(Peer.class, "nodeName");
		peerHosts = accessible(Peer.class, "hosts");
		peerReplaceNode = accessible(Peer.class, "replaceNode");
	}

	@AfterClass
	public static void destroy() {
		if (client != null) {
			client.close();
		}
	}

	@Before
	public void resetNode() throws Exception {
		nodeReferenceCount.setInt(node, 0);
	}

	@After
	public void restoreNode() throws Exception {
		nodeFailures.setInt(node, 0);
		nodeAddress.set(node, origAddress);
		nodeReferenceCount.setInt(node, 0);
	}

	@Test
	public void replaceNodeWithFailuresAndNewAddress() throws Exception {
		setNodeState(1, remoteAddress);

		Peer peer = createPeer(node.getName(), otherHost);
		Peers peers = new Peers(16);

		assertFalse("Peer should be added", findPeerNode(peers, peer));
		assertSame("Existing node should be marked for replacement", node, getReplaceNode(peer));
		assertEquals(0, nodeReferenceCount.getInt(node));
	}

	@Test
	public void keepNodeWithoutFailures() throws Exception {
		setNodeState(0, remoteAddress);

		Peer peer = createPeer(node.getName(), otherHost);
		Peers peers = new Peers(16);

		assertTrue("Peer should be rejected", findPeerNode(peers, peer));
		assertNull("Healthy node must not be replaced", getReplaceNode(peer));
		assertEquals(1, nodeReferenceCount.getInt(node));
	}

	@Test
	public void keepLocalhostNodeWithFailures() throws Exception {
		setNodeState(1, localAddress);

		Peer peer = createPeer(node.getName(), otherHost);
		Peers peers = new Peers(16);

		assertTrue("Peer should be rejected", findPeerNode(peers, peer));
		assertNull("Localhost node must not be replaced", getReplaceNode(peer));
		assertEquals(1, nodeReferenceCount.getInt(node));
	}

	@Test
	public void keepNodeWhenPeerHostMatches() throws Exception {
		setNodeState(1, remoteAddress);

		// One of the peer hosts is the node's current host, so the address has not changed.
		Peer peer = createPeer(node.getName(), otherHost, node.getHost());
		Peers peers = new Peers(16);

		assertTrue("Peer should be rejected", findPeerNode(peers, peer));
		assertNull("Node with matching host must not be replaced", getReplaceNode(peer));
		assertEquals(1, nodeReferenceCount.getInt(node));
	}

	@Test
	public void peerAlreadyAddedInTendIteration() throws Exception {
		setNodeState(1, remoteAddress);

		// Simulate replacement node already created by another node's peers refresh
		// in the same tend iteration.
		Peers peers = new Peers(16);
		peers.nodes.put(node.getName(), node);

		Peer peer = createPeer(node.getName(), otherHost);

		assertTrue("Peer should be rejected", findPeerNode(peers, peer));
		assertNull("Replacement must not be requested twice", getReplaceNode(peer));
		assertEquals(1, nodeReferenceCount.getInt(node));
	}

	@Test
	public void unknownPeerNotReplaced() throws Exception {
		setNodeState(1, remoteAddress);

		Peer peer = createPeer("BB9FFFFFFFFFFFF", otherHost);
		Peers peers = new Peers(16);

		assertFalse("New peer should be added", findPeerNode(peers, peer));
		assertNull("New peer does not replace any node", getReplaceNode(peer));
		assertEquals(0, nodeReferenceCount.getInt(node));
	}

	private static void setNodeState(int failures, InetSocketAddress address) throws Exception {
		nodeFailures.setInt(node, failures);
		nodeAddress.set(node, address);
	}

	private static Peer createPeer(String nodeName, Host... hosts) throws Exception {
		Peer peer = new Peer();
		peerNodeName.set(peer, nodeName);
		peerHosts.set(peer, Arrays.asList(hosts));
		return peer;
	}

	private static boolean findPeerNode(Peers peers, Peer peer) throws Exception {
		return (Boolean)findPeerNode.invoke(null, cluster, peers, peer);
	}

	private static Node getReplaceNode(Peer peer) throws Exception {
		return (Node)peerReplaceNode.get(peer);
	}

	private static Field accessible(Class<?> cls, String name) throws Exception {
		Field field = cls.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}
}
