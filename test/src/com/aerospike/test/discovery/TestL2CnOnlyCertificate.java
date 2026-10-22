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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3 row N-17 against the cn-only profile of test/discovery/run_l2.sh,
 * ee only. The topology is N-6's, but the server certificate carries the tls-name only as its
 * subject CN, with no matching dNSName SAN. In its own class and profile so that a failing N-17
 * does not fail the other rows.
 */
public class TestL2CnOnlyCertificate extends TestBase {
	private static final String PROFILE = " (run_l2.sh DISCOVERY_SUBSET=ee L2_PROFILE=cn-only)";

	@After
	public void restore() {
		Log.setCallback(null);
	}

	/**
	 * N-17: a certificate that carries the tls-name only as its subject CN is rejected. The TLS
	 * handshake with the seed fails on the tls-name and no {@link com.aerospike.client.cluster.Node}
	 * is created.
	 */
	@Test
	public void n17CnOnlyCertificateIsRejected() throws Exception {
		TestL2Topology.skipUnless("ee".equals(L2Topology.subset), "N-17 is ee only: it needs server TLS and TLS listeners" +
			PROFILE + "; subset is " + L2Topology.subset);
		TestL2Topology.skipUnless("cn-only".equals(L2Topology.tlsCertShape), "N-17 needs DISCOVERY_TLS_CERT_SHAPE=cn-only, " +
			"a server certificate with the tls-name only as its CN" + PROFILE + "; shape is " + L2Topology.tlsCertShape);

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		assertTrue("No TLS listener for " + first, first.tlsPort > 0);
		assertNotNull("No tls-name for " + first, first.tlsName);

		ClientPolicy policy = L2Topology.policy();
		policy.tlsPolicy = new TlsPolicy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.tlsPortMap(nodes));
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.host, first.tlsName, first.tlsPort))) {
			System.out.println("L2-N17-ACCEPTED nodes=" + L2Topology.nodeNames(client) + " cnFallbackWarnings=" +
				TestL2Topology.cnFallbackWarnings());
			fail("N-17: TLS handshake accepted a certificate that carries " + first.tlsName + " only as its CN; nodes=" +
				L2Topology.nodeNames(client) + " cnFallbackWarnings=" + TestL2Topology.cnFallbackWarnings());
		}
		catch (AerospikeException ae) {
			String message = TestL2Topology.messages(ae);
			System.out.println("L2-N17-ERROR " + message);
			assertTrue(message, message.contains("Invalid TLS name: " + first.tlsName));
		}
	}
}
