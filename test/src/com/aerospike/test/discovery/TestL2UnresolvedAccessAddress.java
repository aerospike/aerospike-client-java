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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.Info;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.util.TestBase;

/**
 * A node that configures an alternate-access-address which does not resolve in its container,
 * against the unresolved-access profile of test/discovery/run_l2.sh. DISCOVERY_VERBATIM_ACCESS_ADDRESS
 * says whether the server under test advertises access-address hostnames verbatim. When it does, the
 * node joins and advertises the hostname as configured; when it does not, the server resolves the
 * hostname at startup, does not start, and names the address in its log.
 */
public class TestL2UnresolvedAccessAddress extends TestBase {
	private static final String BLOCKER = "Unresolved access address needs DISCOVERY_UNRESOLVED_HOSTNAME and " +
		"DISCOVERY_CONTROL_DIR (run_l2.sh L2_PROFILE=unresolved-access)";
	private static final int STATUS_POLL_MS = 500;

	private String addedProxy;
	private String addedNode;

	@After
	public void restore() throws Exception {
		if (addedProxy != null) {
			L2Topology.removeListener(addedProxy);
			addedProxy = null;
		}

		if (addedNode != null) {
			L2Topology.control("stop " + addedNode);
			addedNode = null;
			L2Topology.waitClusterStable(L2Topology.nodes().size());
		}
	}

	/**
	 * Start a node whose alternate-access-address is DISCOVERY_UNRESOLVED_HOSTNAME with its own
	 * alternate-access-port, and confirm the hostname does not resolve where the node runs. A server
	 * that advertises it verbatim joins the cluster and returns {@code hostname:port} unchanged in
	 * service-clear-alt, and its peers list it there. A server that resolves it exits at startup with
	 * a log line naming the hostname, and the cluster keeps its size.
	 */
	@Test
	public void unresolvedAlternateAccessAddress() throws Exception {
		TestL2Topology.skipUnless(! L2Topology.unresolvedHostname.isEmpty() && ! L2Topology.controlDir.isEmpty(), BLOCKER);

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		String hostname = L2Topology.unresolvedHostname;
		boolean verbatim = "1".equals(L2Topology.verbatimAccessAddress);
		int port = L2Topology.nextListenerPort();
		String endpoint = hostname + ':' + port;
		String container = L2Topology.control("start alternate-port=" + port).get("container");
		addedNode = container;
		System.out.println("L2-UNRESOLVED-START container=" + container + " alternate=" + endpoint +
			" verbatimExpected=" + verbatim);

		if (verbatim) {
			L2Topology.addListener(container, port, container + ':' + L2Topology.upstreamPort(first.proxy));
			addedProxy = container;
			L2Topology.waitClusterStable(nodes.size() + 1);

			Map<String,String> status = status(container);
			assertUnresolved(status);
			assertEquals(container + " running", "true", status.get("running"));

			Map<String,String> info = Info.request(L2Topology.host, port, "node", "service-clear-alt");
			String advertised = info.get("service-clear-alt");
			String peer = L2Topology.peerName(first, "peers-clear-alt", endpoint);
			System.out.println("L2-UNRESOLVED-ADVERTISED node=" + info.get("node") + " service-clear-alt=" + advertised +
				" listedBy=" + first.name + " as=" + peer);
			assertEquals(endpoint, advertised);
			assertEquals(info.get("node"), peer);
		}
		else {
			boolean exited = L2Topology.waitFor(() -> ! "true".equals(status(container).get("running")), STATUS_POLL_MS);
			Map<String,String> status = status(container);
			List<String> lines = logLines(status.get("log"), hostname);

			for (String line : lines) {
				System.out.println("L2-UNRESOLVED-LOG " + line);
			}
			assertTrue(container + " still running after " + L2Topology.WAIT_MS + "ms", exited);
			assertUnresolved(status);
			assertNotEquals(container + " exit code", "0", status.get("exit-code"));
			assertTrue("No CRITICAL log line names " + hostname + ": " + lines,
				lines.stream().anyMatch(line -> line.contains("CRITICAL")));
			L2Topology.waitClusterStable(nodes.size());
		}
	}

	private static void assertUnresolved(Map<String,String> status) {
		System.out.println("L2-UNRESOLVED-LOOKUP getent hosts " + L2Topology.unresolvedHostname + " in=" +
			status.get("lookup-in") + " exit=" + status.get("lookup-exit") + " output=[" + status.get("lookup").trim() + ']');
		assertFalse("No lookup", status.get("lookup-in").isEmpty());
		assertEquals(L2Topology.unresolvedHostname + " resolves: " + status.get("lookup"), "2", status.get("lookup-exit"));
	}

	private static Map<String,String> status(String container) {
		try {
			return L2Topology.control("status " + container);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Return the lines of a server log that name {@code hostname}, other than the config echo.
	 */
	private static List<String> logLines(String log, String hostname) throws IOException {
		List<String> lines = new ArrayList<>();

		for (String line : Files.readAllLines(Paths.get(log), StandardCharsets.UTF_8)) {
			if (line.contains(hostname) && ! line.contains("alternate-access-address " + hostname)) {
				lines.add(line);
			}
		}
		return lines;
	}
}
