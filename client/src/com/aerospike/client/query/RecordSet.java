/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.query;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Iterable set of key/record pairs from a query; call {@link #next()} then {@link #getKeyRecord()} (or iterate) and always {@link #close()} when done.
 * <p>
 * Returned by {@link com.aerospike.client.AerospikeClient#query}. Use {@link #END} to detect end when iterating; {@link #next()} returns false when no more records. Close in a finally block to release resources.
 * <p>Run a query and iterate RecordSet with next() and getKeyRecord().</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("set1");
 * stmt.setFilter(Filter.equal("bin1", 1));
 * RecordSet rs = client.query(null, stmt);
 * try {
 *   while (rs.next()) {
 *     KeyRecord kr = rs.getKeyRecord();
 *     if (kr.record != null) { Object v = kr.record.getValue("mybin"); }
 *   }
 * } finally {
 *   rs.close();
 * }
 * client.close();
 * }</pre>
 *
 * @see KeyRecord
 * @see ResultSet
 * @see QueryListener
 * @see Statement
 * @see com.aerospike.client.AerospikeClient#query
 */
public class RecordSet implements Iterable<KeyRecord>, Closeable {
	/** Sentinel indicating no more records; do not use as application data. */
	public static final KeyRecord END = new KeyRecord(null, null);

	private final IQueryExecutor executor;
	private final BlockingQueue<KeyRecord> queue;
	private KeyRecord record;
	private volatile boolean valid = true;

	/**
	 * Initialize record set with underlying producer/consumer queue.
	 */
	protected RecordSet(IQueryExecutor executor, int capacity) {
		this.executor = executor;
		this.queue = new ArrayBlockingQueue<KeyRecord>(capacity);
	}

	/**
	 * For internal use only.
	 */
	protected RecordSet() {
		this.executor = null;
		this.queue = null;
	}

	//-------------------------------------------------------
	// Record traversal methods
	//-------------------------------------------------------

	/**
	 * Advances to the next record; blocks until a record is available or the query ends.
	 * @return true if a record is available (use {@link #getKeyRecord()}); false when no more records
	 * @throws AerospikeException when the query fails on the server (e.g. timeout, connection error, or invalid statement)
	 */
	public boolean next() throws AerospikeException {
		if (! valid) {
			executor.checkForException();
			return false;
		}

		try {
			record = queue.take();
		}
		catch (InterruptedException ie) {
			valid = false;

			/*
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + executor.statement.taskId + " take interrupted");
			}
			*/
			return false;
		}

		if (record == END) {
			valid = false;
			executor.checkForException();
			return false;
		}
		return true;
	}

	/** Closes this record set and releases resources; call in a finally block. */
	public void close() {
		valid = false;

		// Check if more records are available.
		if (record != END && queue.poll() != END) {
			// Some query threads may still be running. Stop these threads.
			executor.stopThreads(new AerospikeException.QueryTerminated());
		}
	}

	/**
	 * Provide Iterator for RecordSet.
	 */
	@Override
	public Iterator<KeyRecord> iterator() {
		return new RecordSetIterator(this);
	}

	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------

	/**
	 * Get record's unique identifier.
	 */
	public Key getKey() {
		return record.key;
	}

	/**
	 * Get record's header and bin data.
	 */
	public Record getRecord() {
		return record.record;
	}

	/**
	 * Get key and record.
	 */
	public KeyRecord getKeyRecord() {
		return record;
	}

	//-------------------------------------------------------
	// Methods for internal use only.
	//-------------------------------------------------------

	/**
	 * Put a record on the queue.
	 */
	protected final boolean put(KeyRecord record) {
		if (! valid) {
			return false;
		}

		try {
			// This put will block if queue capacity is reached.
			queue.put(record);
			return true;
		}
		catch (InterruptedException ie) {
			/*
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + executor.statement.taskId + " put interrupted");
			}
			*/

			// Valid may have changed.  Check again.
			if (valid) {
				abort();
			}
			return false;
		}
	}

	/**
	 * Abort retrieval with end token.
	 */
	protected void abort() {
		valid = false;
		queue.clear();

		// Send end command to command thread.
		// It's critical that the end offer succeeds.
		while (! queue.offer(END)) {
			// Queue must be full. Remove one item to make room.
			if (queue.poll() == null) {
				// Can't offer or poll.  Nothing further can be done.
				/*
				if (Log.debugEnabled()) {
					Log.debug("RecordSet " + executor.statement.taskId + " both offer and poll failed on abort");
				}
				*/
				break;
			}
		}
	}

	/**
	 * Support standard iteration interface for RecordSet.
	 */
	private static class RecordSetIterator implements Iterator<KeyRecord>, Closeable {

		private final RecordSet recordSet;
		private boolean more;

		RecordSetIterator(RecordSet recordSet) {
			this.recordSet = recordSet;
			more = this.recordSet.next();
		}

		@Override
		public boolean hasNext() {
			return more;
		}

		@Override
		public KeyRecord next() {
			KeyRecord kr = recordSet.getKeyRecord();
			more = recordSet.next();
			return kr;
		}

		@Override
		public void close() {
			recordSet.close();
		}
	}
}
