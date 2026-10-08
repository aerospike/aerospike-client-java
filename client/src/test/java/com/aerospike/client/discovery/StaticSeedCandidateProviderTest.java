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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;

import org.junit.Test;

import com.aerospike.client.Host;

public class StaticSeedCandidateProviderTest {
	@Test
	public void returnsConfiguredHosts() {
		Host h1 = new Host("10.0.0.1", 3000);
		Host h2 = new Host("10.0.0.2", "tls2", 4333);
		List<Host> hosts = new StaticSeedCandidateProvider(h1, h2).refreshSeedCandidates();

		assertEquals(List.of(h1, h2), hosts);
		assertEquals("tls2", hosts.get(1).tlsName);
	}

	@Test
	public void isCopyOfConfiguredHosts() {
		Host[] hosts = new Host[] {new Host("10.0.0.1", 3000)};
		StaticSeedCandidateProvider provider = new StaticSeedCandidateProvider(hosts);
		hosts[0] = new Host("10.0.0.9", 3000);

		assertEquals("10.0.0.1", provider.refreshSeedCandidates().get(0).name);
	}

	@Test
	public void noPeriodicRefreshAndSupportsDeadline() {
		StaticSeedCandidateProvider provider = new StaticSeedCandidateProvider(new Host("10.0.0.1", 3000));

		assertFalse(provider.needsPeriodicRefresh());
		assertTrue(provider.supportsDeadline());
	}

	@Test
	public void startsNoThread() {
		ThreadMXBean bean = ManagementFactory.getThreadMXBean();
		long started = bean.getTotalStartedThreadCount();

		StaticSeedCandidateProvider provider = new StaticSeedCandidateProvider(new Host("10.0.0.1", 3000));
		provider.refreshSeedCandidates();
		provider.needsPeriodicRefresh();
		provider.supportsDeadline();

		assertEquals(started, bean.getTotalStartedThreadCount());
	}
}
