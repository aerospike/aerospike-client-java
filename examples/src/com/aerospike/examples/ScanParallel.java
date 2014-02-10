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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;

public class ScanParallel extends Example implements ScanCallback {

	private int recordCount = 0;

	public ScanParallel(Console console) {
		super(console);
	}

	/**
	 * Scan all nodes in parallel and read all records in a set.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		console.info("Scan parallel: namespace=" + params.namespace + " set=" + params.set);
		recordCount = 0;
		long begin = System.currentTimeMillis();
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(policy, params.namespace, params.set, this);

		long end = System.currentTimeMillis();
		double seconds =  (double)(end - begin) / 1000.0;
		console.info("Total records returned: " + recordCount);
		console.info("Elapsed time: " + seconds + " seconds");
		double performance = Math.round((double)recordCount / seconds);
		console.info("Records/second: " + performance);
	}

	@Override
	public void scanCallback(Key key, Record record) {
		recordCount++;

		if ((recordCount % 10000) == 0) {
			console.info("Records " + recordCount);
		}
	}
}
