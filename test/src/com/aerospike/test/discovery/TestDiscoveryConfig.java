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
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.SuiteDiscovery;
import com.aerospike.test.util.Args;
import com.aerospike.test.util.TestBase;

/**
 * Asserts the discovery config surface reachable through the {@code args} system property:
 * what each flag parses into, which values are rejected, and what reaches the client policy.
 */
public class TestDiscoveryConfig extends TestBase {
	private static final String[] SPI_FLAGS = {
		"--discovery-provider seeds",
		"--translation-provider translator",
		"--endpoint-hostname gateway.local",
		"--endpoint-port-base 4100",
		"--on-miss strict",
		"--execution-mode tend"
	};

	@Test
	public void flagsParseIntoPublicFields() {
		Args a = parse("--discovery-provider seeds --translation-provider translator" +
			" --endpoint-hostname gateway.local --endpoint-port-base 4100" +
			" --on-miss strict --execution-mode tend --use-services-alternate");

		assertEquals("seeds", a.discoveryProvider);
		assertEquals("translator", a.translationProvider);
		assertEquals("gateway.local", a.endpointHostname);
		assertEquals(4100, a.endpointPortBase);
		assertEquals(Args.OnMiss.strict, a.onMiss);
		assertEquals(Args.ExecutionMode.tend, a.executionMode);
		assertTrue(a.useServicesAlternate);
	}

	@Test
	public void defaultsMatchFrameworkDefaults() {
		Args a = parse("-h 127.0.0.1 -p 3000 -n test");

		assertNull(a.discoveryProvider);
		assertNull(a.translationProvider);
		assertNull(a.endpointHostname);
		assertEquals(0, a.endpointPortBase);
		assertEquals(Args.OnMiss.passThrough, a.onMiss);
		assertEquals(Args.ExecutionMode.thread, a.executionMode);
		assertFalse(a.useServicesAlternate);
	}

	@Test
	public void declaredEnumValuesParse() {
		assertEquals(Args.OnMiss.passThrough, parse("--on-miss passThrough").onMiss);
		assertEquals(Args.OnMiss.strict, parse("--on-miss strict").onMiss);
		assertEquals(Args.ExecutionMode.thread, parse("--execution-mode thread").executionMode);
		assertEquals(Args.ExecutionMode.tend, parse("--execution-mode tend").executionMode);
	}

	@Test
	public void unknownOnMissValueIsRejected() {
		assertParseFailure("--on-miss bogus");
	}

	@Test
	public void unknownExecutionModeValueIsRejected() {
		assertParseFailure("--execution-mode bogus");
	}

	@Test
	public void discoveryFlagsAreRejectedByName() {
		for (String flag : SPI_FLAGS) {
			String name = flag.substring(0, flag.indexOf(' '));
			Args a = parse(flag);

			try {
				a.setClientPolicy(new ClientPolicy());
				fail("Expected " + name + " to be rejected");
			}
			catch (AerospikeException ae) {
				assertTrue(name + " not named in: " + ae.getMessage(), ae.getMessage().contains(name));
			}
		}
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
