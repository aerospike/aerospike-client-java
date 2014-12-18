package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Packer;

public final class AsyncQuery extends AsyncMultiCommand {
	private final QueryPolicy policy;
	private final Statement statement;
	private final RecordSequenceListener listener;
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	private final long taskId;

	public AsyncQuery(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			QueryPolicy policy,
			Statement statement,
			RecordSequenceListener listener,
			String namespace,
			String setName,
			String[] binNames,
			long taskId
		) {
			super(parent, cluster, node, true);
			this.policy = policy;
			this.statement = statement;
			this.listener = listener;
			this.namespace = namespace;
			this.setName = setName;
			this.binNames = binNames;
			this.taskId = taskId;
		}
	
	@Override
	protected Policy getPolicy() {
		return policy;
	}
	
	@Override
	protected void writeBuffer() throws AerospikeException {
		setQuery(policy, statement, namespace, setName, binNames, taskId);
	}
	
	@Override
	protected void parseRow(Key key) throws AerospikeException {		
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
}
