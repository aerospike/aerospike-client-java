/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.test.sync.large;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.test.sync.TestSync;

public class TestLargeMap extends TestSync {
	@Test
	public void largeMap() {
		if (! args.validateLDT()) {
			return;
		}
		Key key = new Key(args.namespace, args.set, "setkey");
		String binName = args.getBinName("setbin");
		
		// Delete record if it already exists.
		client.delete(null, key);
		
		// Initialize Large Map operator.
		com.aerospike.client.large.LargeMap lmap = client.getLargeMap(null, key, binName, null);
						
		// Write values.
		lmap.put(Value.get("lmapName1"), Value.get("lmapValue1"));
		lmap.put(Value.get("lmapName2"), Value.get("lmapValue2"));
		lmap.put(Value.get("lmapName3"), Value.get("lmapValue3"));
		
		// Remove last value.
		lmap.remove(Value.get("lmapName3"));
		assertEquals(2, lmap.size());
		
		Map<?,?> mapReceived = lmap.get(Value.get("lmapName2"));
		String stringReceived = (String) mapReceived.get("lmapName2");
		assertEquals("lmapValue2", stringReceived);
	}	
}
