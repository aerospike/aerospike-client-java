package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;

public final class AsyncQuery extends AsyncMultiCommand {
	private final QueryPolicy policy;
	private final Statement statement;
	private final RecordSequenceListener listener;
	private final long taskId;

	public AsyncQuery(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			QueryPolicy policy,
			Statement statement,
			RecordSequenceListener listener,
			long taskId
		) {
			super(parent, cluster, node, true);
			this.policy = policy;
			this.statement = statement;
			this.listener = listener;
			this.taskId = taskId;
		}
	
	@Override
	protected Policy getPolicy() {
		return policy;
	}
	
	@Override
	protected void writeBuffer() throws AerospikeException {
		statement.prepare();
		setQuery(policy, statement, taskId);
	}
	
	@Override
	protected void parseRow(Key key) throws AerospikeException {
		Record record = parseRecord();
		listener.onRecord(key, record);
	}
	
}
