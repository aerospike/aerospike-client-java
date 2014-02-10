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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;

public final class AsyncScan extends AsyncMultiCommand {
	private final ScanPolicy policy;
	private final RecordSequenceListener listener;
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	
	public AsyncScan(
		AsyncMultiExecutor parent,
		AsyncCluster cluster,
		AsyncNode node,
		ScanPolicy policy,
		RecordSequenceListener listener,
		String namespace,
		String setName,
		String[] binNames
	) {
		super(parent, cluster, node, true);
		this.policy = policy;
		this.listener = listener;
		this.namespace = namespace;
		this.setName = setName;
		this.binNames = binNames;
	}
		
	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setScan(policy, namespace, setName, binNames);
	}

	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
}
