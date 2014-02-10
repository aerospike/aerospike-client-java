/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.examples;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.task.RegisterTask;

public class UserDefinedFunction extends Example {

	public UserDefinedFunction(Console console) {
		super(console);
	}

	/**
	 * Register user defined function and call it.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("User defined functions are not supported by the connected Aerospike server.");
			return;
		}
		register(client, params);
		writeUsingUdf(client, params);
		writeIfGenerationNotChanged(client, params);
		writeIfNotExists(client, params);
		writeWithValidation(client, params);
		writeListMapUsingUdf(client, params);
		writeBlobUsingUdf(client, params);
	}
	
	private void register(AerospikeClient client, Parameters params) throws Exception {
		RegisterTask task = client.register(params.policy, "udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	private void writeUsingUdf(AerospikeClient client, Parameters params) throws Exception {	
		Key key = new Key(params.namespace, params.set, "udfkey1");
		Bin bin = new Bin(params.getBinName("udfbin1"), "string value");		
		
		client.execute(params.writePolicy, key, "record_example", "writeBin", Value.get(bin.name), bin.value);
		
		Record record = client.get(params.policy, key, bin.name);
		String expected = bin.value.toString();	
		String received = (String)record.getValue(bin.name);

		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}
	
	private void writeIfGenerationNotChanged(AerospikeClient client, Parameters params) throws Exception {	
		Key key = new Key(params.namespace, params.set, "udfkey2");
		Bin bin = new Bin(params.getBinName("udfbin2"), "string value");		
		
		// Seed record.
		client.put(params.writePolicy, key, bin);
		
		// Get record generation.
		int gen = (Integer)client.execute(params.policy, key, "record_example", "getGeneration");

		// Write record if generation has not changed.
		client.execute(params.writePolicy, key, "record_example", "writeIfGenerationNotChanged", Value.get(bin.name), bin.value, Value.get(gen));		
		console.info("Record written.");
	}

	private void writeIfNotExists(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey3");
		String binName = "udfbin3";
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		// Write record only if not already exists. This should succeed.
		client.execute(params.writePolicy, key, "record_example", "writeUnique", Value.get(binName), Value.get("first"));

		// Verify record written.
		Record record = client.get(params.policy, key, binName);
		String expected = "first";	
		String received = (String)record.getValue(binName);

		if (received != null && received.equals(expected)) {
			console.info("Record written: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, binName, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}

		// Write record second time. This should fail.
		console.info("Attempt second write.");
		client.execute(params.writePolicy, key, "record_example", "writeUnique", Value.get(binName), Value.get("second"));
		
		// Verify record not written.
		record = client.get(params.policy, key, binName);
		received = (String)record.getValue(binName);

		if (received != null && received.equals(expected)) {
			console.info("Success. Record remained unchanged: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, binName, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}

	private void writeWithValidation(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey4");
		String binName = "udfbin4";
				
		// Lua function writeWithValidation accepts number between 1 and 10.
		// Write record with valid value.
		console.info("Write with valid value.");
		client.execute(params.writePolicy, key, "record_example", "writeWithValidation", Value.get(binName), Value.get(4));

		// Write record with invalid value.
		console.info("Write with invalid value.");
		
		try {
			client.execute(params.writePolicy, key, "record_example", "writeWithValidation", Value.get(binName), Value.get(11));
			console.error("UDF should not have succeeded!");
		}
		catch (Exception e) {
			console.info("Success. UDF resulted in exception as expected.");
		}
	}

	private void writeListMapUsingUdf(AerospikeClient client, Parameters params) throws Exception {	
		Key key = new Key(params.namespace, params.set, "udfkey5");

		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(8L);
		
		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1L);
		innerMap.put(2L, "b");
		innerMap.put("list", inner);
		
		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(4L);
		list.add(inner);
		list.add(innerMap);

		String binName = params.getBinName("udfbin5");

		client.execute(params.policy, key, "record_example", "writeBin", Value.get(binName), Value.getAsList(list));
		
		Object received = client.execute(params.policy, key, "record_example", "readBin", Value.get(binName));

		if (received != null && received.equals(list)) {
			console.info("UDF data matched: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, binName, received);
		}
		else {
			console.error("UDF data mismatch");
			console.error("Expected " + list);
			console.error("Received " + received);
		}
	}
	
	private void writeBlobUsingUdf(AerospikeClient client, Parameters params) throws Exception {	
		Key key = new Key(params.namespace, params.set, "udfkey6");
		String binName = params.getBinName("udfbin6");

		// Create packed blob using standard java tools.
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeInt(9845);
		dos.writeUTF("Hello world.");
		byte[] blob = baos.toByteArray();
		
		client.execute(params.policy, key, "record_example", "writeBin", Value.get(binName), Value.get(blob));
		byte[] received = (byte[])client.execute(params.policy, key, "record_example", "readBin", Value.get(binName));

		if (Arrays.equals(blob, received)) {
			console.info("Blob data matched: namespace=%s set=%s key=%s bin=%s value=%s", 
					key.namespace, key.setName, key.userKey, binName, Arrays.toString(received));
		}
		else {
			throw new Exception(String.format(
				"Mismatch: expected=%s received=%s", Arrays.toString(blob), Arrays.toString(received))); 
		}
	}	
}
