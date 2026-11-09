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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aerospike.client.Log;

/**
 * The L5 driver's log filter. Needs no server.
 */
public class TestL5DriverLog {
	@Test
	public void forwardsNodeEventsAtInfo() {
		assertTrue(L5Driver.forward(Log.Level.INFO, "Add node BB9A000000000001 10.73.1.72 3000"));
		assertTrue(L5Driver.forward(Log.Level.INFO, "Remove node BB9A000000000001 10.73.1.72 3000"));
		assertTrue(L5Driver.forward(Log.Level.INFO, "Replace node: BB9A000000000001 10.73.1.72 3000"));
		assertTrue(L5Driver.forward(Log.Level.INFO, "Move node BB9A000000000001 a.example from 10.73.1.72 3000 to 10.73.2.106 3000"));
	}

	@Test
	public void dropsOtherInfo() {
		assertFalse(L5Driver.forward(Log.Level.INFO, "Login to BB9A000000000001 10.73.1.72 3000"));
		assertFalse(L5Driver.forward(Log.Level.INFO, "Skip orphan node: BB9A000000000001 10.73.1.72 3000"));
		assertFalse(L5Driver.forward(Log.Level.INFO, "Add seed 10.73.1.72 3000"));
		assertFalse(L5Driver.forward(Log.Level.INFO, "Node BB9A000000000001 added"));
		assertFalse(L5Driver.forward(Log.Level.INFO, "add node BB9A000000000001 10.73.1.72 3000"));
	}

	@Test
	public void forwardsWarnAndErrorOnly() {
		assertTrue(L5Driver.forward(Log.Level.WARN, "Node BB9A000000000001 10.73.1.72 3000 refresh failed: timeout"));
		assertTrue(L5Driver.forward(Log.Level.ERROR, "Invalid peer received by cluster tend: a.example"));
		assertFalse(L5Driver.forward(Log.Level.DEBUG, "Add node BB9A000000000001 10.73.1.72 3000"));
		assertFalse(L5Driver.forward(Log.Level.DEBUG, "Update peers for node BB9A000000000001 10.73.1.72 3000"));
	}
}
