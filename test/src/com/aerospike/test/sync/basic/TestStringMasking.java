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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.Role;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.operation.StringOperation;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.sync.TestSync;

/**
 * Integration tests for string operations applied to bins protected by a
 * server-side masking rule.
 *
 * <p>Each test exercises one privilege boundary:
 * <ul>
 *   <li>read with the {@code read-masked} privilege should observe the real value;
 *   <li>read without it should observe the masked value;
 *   <li>modify without {@code write-masked} should fail with
 *       {@link ResultCode#ROLE_VIOLATION}.
 * </ul>
 *
 * <p>The test bootstraps two extra users (one privileged reader, one
 * unprivileged user) and connects an additional client per role. The whole
 * class is skipped when security is disabled, no admin credentials are
 * supplied, or the cluster is older than 8.1.3 (where masking + string ops
 * are jointly supported).
 */
public class TestStringMasking extends TestSync {
	private static final String MASKED_BIN = "pii";
	private static final String UNMASKED_BIN = "public";
	private static final String INITIAL_VALUE = "hello world";
	private static final String INITIAL_PUBLIC = "visible data";
	private static final String MASK_FUNCTION = "redact";

	private static final String PRIV_USER = "stringops_reader";
	private static final String UNPRIV_USER = "stringops_user";
	private static final String USER_PASSWORD = "stringops_pw1!";

	private static final Key KEY = new Key(args.namespace, args.set, "stringmask-key");
	private static final StringPolicy POLICY = StringPolicy.Default;

	private static boolean enabled;
	private static IAerospikeClient privClient;
	private static IAerospikeClient unprivClient;

	@BeforeClass
	public static void setupUsersAndRule() {
		Assume.assumeTrue("Skipping: server version < 8.1.3 (string ops + masking)",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));
		Assume.assumeTrue("Skipping: admin credentials not provided",
			args.user != null && !args.user.isEmpty()
				&& args.password != null && !args.password.isEmpty());

		// Probe the cluster for security; bail out cleanly if it isn't enabled.
		try {
			client.queryRoles(new AdminPolicy());
		}
		catch (AerospikeException e) {
			if (e.getResultCode() == ResultCode.SECURITY_NOT_ENABLED
				|| e.getResultCode() == ResultCode.SECURITY_NOT_SUPPORTED
				|| e.getResultCode() == ResultCode.NOT_AUTHENTICATED) {
				Assume.assumeTrue("Skipping: security not enabled on cluster", false);
			}
			throw e;
		}

		dropUserQuiet(PRIV_USER);
		dropUserQuiet(UNPRIV_USER);

		AdminPolicy ap = new AdminPolicy();
		client.createUser(ap, PRIV_USER, USER_PASSWORD,
			Arrays.asList(Role.ReadWrite, Role.ReadMasked));
		client.createUser(ap, UNPRIV_USER, USER_PASSWORD,
			Arrays.asList(Role.ReadWrite));

		privClient = newClient(PRIV_USER);
		unprivClient = newClient(UNPRIV_USER);

		applyMaskRule(MASKED_BIN, MASK_FUNCTION, null);
		enabled = true;
	}

	@AfterClass
	public static void tearDown() {
		if (!enabled) {
			return;
		}
		removeMaskRule(MASKED_BIN);

		try {
			AdminPolicy ap = new AdminPolicy();
			dropUserQuiet(PRIV_USER);
			dropUserQuiet(UNPRIV_USER);
			// If queryRoles fires (it can race role propagation), let close still run.
			client.queryRoles(ap);
		}
		catch (Exception ignored) {
		}
		finally {
			closeQuiet(privClient);
			closeQuiet(unprivClient);
		}
	}

	@Before
	public void resetRecord() {
		client.delete(null, KEY);
		client.put(null, KEY,
			new Bin(MASKED_BIN, INITIAL_VALUE),
			new Bin(UNMASKED_BIN, INITIAL_PUBLIC));
	}

	//=================================================================
	// Read ops: privilege gates which value the caller observes
	//=================================================================

	@Test
	public void readMaskedSeesRealValue_strlen() {
		Record r = privClient.operate(null, KEY, StringOperation.strlen(MASKED_BIN));
		assertEquals(INITIAL_VALUE.length(), r.getLong(MASKED_BIN));
	}

	@Test
	public void readMaskedSeesRealValue_substr() {
		Record r = privClient.operate(null, KEY, StringOperation.substr(MASKED_BIN, 0, 5));
		assertEquals("hello", r.getString(MASKED_BIN));
	}

	@Test
	public void unprivilegedSeesMaskedSubstring() {
		Record r = unprivClient.operate(null, KEY, StringOperation.substr(MASKED_BIN, 0, 5));
		// A full-redact rule should never let the underlying characters leak.
		String value = r.getString(MASKED_BIN);
		assertEquals(5, value.length());
		assertNotEquals("hello", value);
	}

	@Test
	public void unprivilegedFindOnMaskedBinDoesNotLocateRealContent() {
		Record r = unprivClient.operate(null, KEY, StringOperation.find(MASKED_BIN, "world"));
		assertEquals(-1L, r.getLong(MASKED_BIN));
	}

	@Test
	public void unprivilegedContainsOnMaskedBinIsFalse() {
		Record r = unprivClient.operate(null, KEY, StringOperation.contains(MASKED_BIN, "hello"));
		assertEquals(0L, r.getLong(MASKED_BIN));
	}

	@Test
	public void unprivilegedStartsEndsOnMaskedBinAreFalse() {
		Record sw = unprivClient.operate(null, KEY, StringOperation.startsWith(MASKED_BIN, "hello"));
		Record ew = unprivClient.operate(null, KEY, StringOperation.endsWith(MASKED_BIN, "world"));
		assertEquals(0L, sw.getLong(MASKED_BIN));
		assertEquals(0L, ew.getLong(MASKED_BIN));
	}

	@Test
	public void unprivilegedRegexCompareOnMaskedBinDoesNotMatchReal() {
		Record r = unprivClient.operate(null, KEY, StringOperation.regexCompare(MASKED_BIN, "hello.*"));
		assertEquals(0L, r.getLong(MASKED_BIN));
	}

	@Test
	public void strlenIsUnaffectedByRedaction() {
		// Redact preserves length, so both clients agree on strlen/byteLength.
		Record priv = privClient.operate(null, KEY, StringOperation.byteLength(MASKED_BIN));
		Record unp = unprivClient.operate(null, KEY, StringOperation.byteLength(MASKED_BIN));
		assertEquals(INITIAL_VALUE.length(), priv.getLong(MASKED_BIN));
		assertEquals(INITIAL_VALUE.length(), unp.getLong(MASKED_BIN));
	}

	//=================================================================
	// Read ops on the unmasked bin — both users see the real data
	//=================================================================

	@Test
	public void unmaskedBinIsTransparentToBothUsers() {
		Record priv = privClient.operate(null, KEY, StringOperation.strlen(UNMASKED_BIN));
		Record unp = unprivClient.operate(null, KEY, StringOperation.strlen(UNMASKED_BIN));
		assertEquals(INITIAL_PUBLIC.length(), priv.getLong(UNMASKED_BIN));
		assertEquals(INITIAL_PUBLIC.length(), unp.getLong(UNMASKED_BIN));
	}

	//=================================================================
	// Modify ops: blocked without write-masked
	//=================================================================

	@Test
	public void writeMaskedRequired_upper() {
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.upper(POLICY, MASKED_BIN)));
	}

	@Test
	public void writeMaskedRequired_insert() {
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.insert(POLICY, MASKED_BIN, 5, " beautiful")));
	}

	@Test
	public void writeMaskedRequired_concat() {
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.concat(POLICY, MASKED_BIN, "!")));
	}

	@Test
	public void writeMaskedRequired_replace() {
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.replace(POLICY, MASKED_BIN, "world", "earth")));
	}

	@Test
	public void writeMaskedRequired_trim() {
		client.put(null, KEY, new Bin(MASKED_BIN, "  padded  "));
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.trim(POLICY, MASKED_BIN)));
	}

	@Test
	public void writeMaskedRequired_padStart() {
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.padStart(POLICY, MASKED_BIN, 20, "*")));
	}

	@Test
	public void writeMaskedRequired_regexReplace() {
		assertRoleViolation(() -> unprivClient.operate(null, KEY,
			StringOperation.regexReplace(POLICY, MASKED_BIN, "[0-9]+", "NUM", 0)));
	}

	//=================================================================
	// Read-masked still cannot modify; admin still can.
	//=================================================================

	@Test
	public void readMaskedCannotModify() {
		assertRoleViolation(() -> privClient.operate(null, KEY,
			StringOperation.upper(POLICY, MASKED_BIN)));
	}

	@Test
	public void adminModifyOnMaskedBinSucceeds() {
		client.operate(null, KEY, StringOperation.upper(POLICY, MASKED_BIN));
		Record r = client.get(null, KEY);
		assertEquals("HELLO WORLD", r.getString(MASKED_BIN));
	}

	//=================================================================
	// Modify on unmasked bin succeeds for unprivileged user.
	//=================================================================

	@Test
	public void unprivilegedCanModifyUnmaskedBin() {
		unprivClient.operate(null, KEY, StringOperation.upper(POLICY, UNMASKED_BIN));
		Record r = client.get(null, KEY);
		assertEquals("VISIBLE DATA", r.getString(UNMASKED_BIN));
		// The masked bin is left untouched.
		assertEquals(INITIAL_VALUE, r.getString(MASKED_BIN));
	}

	//=================================================================
	// Constant-mask variant: unprivileged sees a fixed string
	//=================================================================

	@Test
	public void constantMaskIsObservedByUnprivilegedRead() {
		final String constBin = "secret";
		final String constValue = "HIDDEN";
		final String real = "real secret data";
		final Key key = new Key(args.namespace, args.set, "stringmask-const");

		applyMaskRule(constBin, "constant", "value=" + constValue);
		try {
			client.delete(null, key);
			client.put(null, key, new Bin(constBin, real));

			Record priv = privClient.operate(null, key, StringOperation.strlen(constBin));
			Record unp = unprivClient.operate(null, key, StringOperation.strlen(constBin));
			assertEquals(real.length(), priv.getLong(constBin));
			assertEquals(constValue.length(), unp.getLong(constBin));

			Record privSub = privClient.operate(null, key, StringOperation.substr(constBin, 0, 4));
			Record unpSub = unprivClient.operate(null, key, StringOperation.substr(constBin, 0, 4));
			assertEquals("real", privSub.getString(constBin));
			assertEquals("HIDD", unpSub.getString(constBin));
		}
		finally {
			client.delete(null, key);
			removeMaskRule(constBin);
		}
	}

	//=================================================================
	// Helpers
	//=================================================================

	private interface OperateCall {
		void run();
	}

	private static void assertRoleViolation(OperateCall call) {
		try {
			call.run();
			fail("Expected ROLE_VIOLATION");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.ROLE_VIOLATION, e.getResultCode());
		}
	}

	private static IAerospikeClient newClient(String user) {
		ClientPolicy p = new ClientPolicy();
		args.setClientPolicy(p);
		p.user = user;
		p.password = USER_PASSWORD;
		return new AerospikeClient(p, Host.parseHosts(args.host, args.port));
	}

	private static void closeQuiet(IAerospikeClient c) {
		if (c != null) {
			try { c.close(); } catch (Exception ignored) {}
		}
	}

	private static void dropUserQuiet(String user) {
		try {
			client.dropUser(new AdminPolicy(), user);
		}
		catch (AerospikeException ignored) {
			// User did not exist; nothing to do.
		}
	}

	/**
	 * Apply a masking rule via info command. Format:
	 * {@code masking:namespace=NS;set=SET;bin=BIN;type=string;function=FN[;extra]}
	 */
	private static void applyMaskRule(String bin, String function, String extra) {
		StringBuilder cmd = new StringBuilder("masking:namespace=")
			.append(args.namespace)
			.append(";set=").append(args.set)
			.append(";bin=").append(bin)
			.append(";type=string;function=").append(function);
		if (extra != null && !extra.isEmpty()) {
			cmd.append(';').append(extra);
		}
		infoOnAllNodes(cmd.toString());
	}

	private static void removeMaskRule(String bin) {
		String cmd = "masking:namespace=" + args.namespace
			+ ";set=" + args.set
			+ ";bin=" + bin
			+ ";type=string;function=remove";
		infoOnAllNodes(cmd);
	}

	private static void infoOnAllNodes(String cmd) {
		List<Node> nodes = Arrays.asList(client.getNodes());
		for (Node node : nodes) {
			Info.request(null, node, cmd);
		}
		// Give the rule time to propagate before exercising it.
		try { Thread.sleep(500); } catch (InterruptedException ignored) {}
	}

}
