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
package com.aerospike.client.async;

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.InfoListener;

public final class AsyncQueryValidate {

	public interface BeginListener {
		void onSuccess(long clusterKey);
		void onFailure(AerospikeException ae);
	}

	public static void validateBegin(
		Cluster cluster,
		EventLoop eventLoop,
		AsyncQueryValidate.BeginListener listener,
		Node node,
		String namespace
	) {
		String command = "cluster-stable:namespace=" + namespace;
		AsyncInfoCommand aic = new AsyncInfoCommand(new BeginHandler(listener, command), null, node, command);
		eventLoop.execute(cluster, aic);
	}

	private static class BeginHandler implements InfoListener {
		private final AsyncQueryValidate.BeginListener listener;
		private final String command;

		private BeginHandler(AsyncQueryValidate.BeginListener listener, String command) {
			this.listener = listener;
			this.command = command;
		}

		@Override
		public void onSuccess(Map<String,String> map) {
			String result = map.get(command);
			long clusterKey = 0;

			try {
				clusterKey = Long.parseLong(result, 16);
			}
			catch (Exception e) {
				// Yes, even scans return QUERY_ABORTED.
				listener.onFailure(new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + result));
				return;
			}

			listener.onSuccess(clusterKey);
		}

		@Override
		public void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	public interface Listener {
		void onSuccess();
		void onFailure(AerospikeException ae);
	}

	public static void validate(
		Cluster cluster,
		EventLoop eventLoop,
		AsyncQueryValidate.Listener listener,
		Node node,
		String namespace,
		long expectedKey
	) {
		String command = "cluster-stable:namespace=" + namespace;
		AsyncInfoCommand aic = new AsyncInfoCommand(new Handler(listener, command, expectedKey), null, node, command);
		eventLoop.execute(cluster, aic);
	}

	private static class Handler implements InfoListener {
		private final AsyncQueryValidate.Listener listener;
		private final String command;
		private final long expectedKey;

		private Handler(AsyncQueryValidate.Listener listener, String command, long expectedKey) {
			this.listener = listener;
			this.command = command;
			this.expectedKey = expectedKey;
		}

		@Override
		public void onSuccess(Map<String,String> map) {
			String result = map.get(command);
			long clusterKey = 0;

			try {
				clusterKey = Long.parseLong(result, 16);
			}
			catch (Exception e) {
				// Yes, even scans return QUERY_ABORTED.
				listener.onFailure(new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + result));
				return;
			}

			if (clusterKey != expectedKey) {
				listener.onFailure(new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + expectedKey + ' ' + clusterKey));
				return;
			}

			listener.onSuccess();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}
}
