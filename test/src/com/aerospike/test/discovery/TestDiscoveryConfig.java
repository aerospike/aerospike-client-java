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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.SuiteDiscovery;
import com.aerospike.test.util.Args;
import com.aerospike.test.util.TestBase;

/**
 * Asserts the discovery config surface reachable through the {@code args} system property:
 * what each flag parses into, which values are rejected, and what reaches the client policy.
 */
public class TestDiscoveryConfig extends TestBase {
	@Test
	public void flagsParseIntoPublicFields() {
		Args a = parse("--discovery-provider seeds --translation-provider translator" +
			" --on-miss strict --use-services-alternate");

		assertEquals("seeds", a.discoveryProvider);
		assertEquals("translator", a.translationProvider);
		assertEquals(TranslationMissMode.STRICT, a.onMiss);
		assertTrue(a.useServicesAlternate);
	}

	@Test
	public void defaultsMatchFrameworkDefaults() {
		Args a = parse("-h 127.0.0.1 -p 3000 -n test");

		assertNull(a.discoveryProvider);
		assertNull(a.translationProvider);
		assertEquals(new ClientPolicy().translationOnMiss, a.onMiss);
		assertFalse(a.useServicesAlternate);
	}

	@Test
	public void declaredOnMissValuesParse() {
		assertEquals(TranslationMissMode.PASS_THROUGH, parse("--on-miss pass_through").onMiss);
		assertEquals(TranslationMissMode.STRICT, parse("--on-miss strict").onMiss);
	}

	@Test
	public void unknownOnMissValueIsRejected() {
		assertParseFailure("--on-miss bogus");
	}

	@Test
	public void camelCaseOnMissValueIsRejected() {
		assertParseFailure("--on-miss passThrough");
	}

	@Test
	public void removedFlagsAreRejected() {
		assertParseFailure("--endpoint-hostname gateway.local");
		assertParseFailure("--endpoint-port-base 4100");
		assertParseFailure("--execution-mode tend");
	}

	@Test
	public void discoveryFlagsDoNotFailClientPolicy() {
		ClientPolicy p = new ClientPolicy();
		parse("--discovery-provider seeds --translation-provider translator").setClientPolicy(p);
		assertNull(p.seedCandidateProvider);
		assertNull(p.addressTranslator);
	}

	@Test
	public void onMissAppliesToClientPolicy() {
		ClientPolicy p = new ClientPolicy();
		parse("--on-miss strict").setClientPolicy(p);
		assertEquals(TranslationMissMode.STRICT, p.translationOnMiss);
	}

	@Test
	public void useServicesAlternateReachesSuiteClientPolicy() {
		assertEquals(args.useServicesAlternate, SuiteDiscovery.clientPolicy.useServicesAlternate);
	}

	@Test
	public void useServicesAlternateAppliesToClientPolicy() {
		ClientPolicy p = new ClientPolicy();
		parse("--use-services-alternate").setClientPolicy(p);
		assertTrue(p.useServicesAlternate);
	}

	private static void assertParseFailure(String argString) {
		try {
			parse(argString);
			fail("Expected parse failure: " + argString);
		}
		catch (AerospikeException ae) {
			assertTrue(ae.getMessage(), ae.getMessage().contains("Failed to parse args: " + argString));
		}
	}

	private static Args parse(String argString) {
		String previous = System.getProperty("args");
		System.setProperty("args", argString);

		try {
			return new Args();
		}
		finally {
			if (previous != null) {
				System.setProperty("args", previous);
			}
			else {
				System.clearProperty("args");
			}
		}
	}
}
