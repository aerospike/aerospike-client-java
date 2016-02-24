/*
 * Copyright 2012-2016 Aerospike, Inc.
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

import java.util.List;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.test.sync.TestSync;

public class TestLargeStack extends TestSync {
	@Test
	public void largeStack() {
		if (! args.validateLDT()) {
			return;
		}
		Key key = new Key(args.namespace, args.set, "stackkey");
		String binName = args.getBinName("stackbin");
		
		// Delete record if it already exists.
		client.delete(null, key);
		
		// Initialize large stack operator.
		com.aerospike.client.large.LargeStack stack = client.getLargeStack(null, key, binName, null);
				
		// Write values.
		stack.push(Value.get("stackvalue1"));
		stack.push(Value.get("stackvalue2"));
		//stack.push(Value.get("stackvalue3"));
					
		// Delete last value.
		// Comment out until trim supported on server.
		//stack.trim(1);
		
		assertEquals(2, stack.size());				
		
		List<?> list = stack.peek(1);
		String received = (String)list.get(0);
		assertEquals("stackvalue2", received);				
	}	
}
