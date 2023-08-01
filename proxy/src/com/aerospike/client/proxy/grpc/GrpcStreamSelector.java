/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.proxy.grpc;

import java.util.List;

/**
 * A selector of streams within a channel to execute Aerospike proxy gRPC calls.
 */
public interface GrpcStreamSelector {
	/**
	 * Select a stream for the gRPC method. All streams created by the
	 * selector should be close when the selector is closed.
	 *
	 * @param streams streams to select from.
	 * @param call    the streaming call to be executed.
	 * @return the selected stream, <code>null</code> when no stream is
	 * selected.
	 */
	SelectedStream select(List<GrpcStream> streams, GrpcStreamingCall call);


	class SelectedStream {
		/**
		 * Wil be non-null only when a current stream is selected.
		 */
		private final GrpcStream stream;

		// Following fields only applies when {@link #stream} is
		// <code>null</code>

		private final int maxConcurrentRequestsPerStream;
		private final int totalRequestsPerStream;

		/**
		 * Create a new stream with the supplied parameters.
		 */
		public SelectedStream(int maxConcurrentRequestsPerStream, int totalRequestsPerStream) {
			this.stream = null;
			this.maxConcurrentRequestsPerStream = maxConcurrentRequestsPerStream;
			this.totalRequestsPerStream = totalRequestsPerStream;
		}

		/**
		 * Use an existing stream.
		 */
		public SelectedStream(GrpcStream stream) {
			this.stream = stream;
			this.maxConcurrentRequestsPerStream = 0;
			this.totalRequestsPerStream = 0;
		}

		boolean useExistingStream() {
			return stream != null;
		}

		public GrpcStream getStream() {
			return stream;
		}

		public int getMaxConcurrentRequestsPerStream() {
			return maxConcurrentRequestsPerStream;
		}

		public int getTotalRequestsPerStream() {
			return totalRequestsPerStream;
		}
	}
}
