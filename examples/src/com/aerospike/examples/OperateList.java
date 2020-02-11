/*
 * Copyright 2012-2020 Aerospike, Inc.
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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;

public class OperateList extends Example {

	public OperateList(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a list bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) {
		if (! params.hasCDTList) {
			console.info("CDT list functions are not supported by the connected Aerospike server.");
			return;
		}
		runSimpleExample(client, params);
		runNestedExample(client, params);
	}

	/**
	 * Simple example of list functionality.
	 */
	public void runSimpleExample(AerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "listkey");
		String binName = params.getBinName("listbin");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(55));
		inputList.add(Value.get(77));

		// Write values to empty list.
		Record record = client.operate(params.writePolicy, key,
				ListOperation.appendItems(binName, inputList)
				);

		console.info("Record: " + record);

		// Pop value from end of list and also return new size of list.
		record = client.operate(params.writePolicy, key,
				ListOperation.pop(binName, -1),
				ListOperation.size(binName)
				);

		console.info("Record: " + record);

		// There should be one result for each list operation on the same list bin.
		// In this case, there are two list operations (pop and size), so there
		// should be two results.
		List<?> list = record.getList(binName);

		for (Object value : list) {
			console.info("Received: " + value);
		}
	}

	/**
	 * Operate on a list of lists.
	 */
	public void runNestedExample(AerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "listkey2");
		String binName = params.getBinName("listbin");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(7));
		l1.add(Value.get(9));
		l1.add(Value.get(5));

		List<Value> l2 = new ArrayList<Value>();
		l2.add(Value.get(1));
		l2.add(Value.get(2));
		l2.add(Value.get(3));

		List<Value> l3 = new ArrayList<Value>();
		l3.add(Value.get(6));
		l3.add(Value.get(5));
		l3.add(Value.get(4));
		l3.add(Value.get(1));

		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(l1));
		inputList.add(Value.get(l2));
		inputList.add(Value.get(l3));

		// Create list.
		client.put(params.writePolicy, key, new Bin(binName, inputList));

		// Append value to last list and retrieve all lists.
		Record record = client.operate(params.writePolicy, key,
				ListOperation.append(binName, Value.get(11), CTX.listIndex(-1)),
				Operation.get(binName)
				);

		record = client.get(params.policy, key);
		console.info("Record: " + record);
	}
}
