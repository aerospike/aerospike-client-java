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
package com.aerospike.client.async;

import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.listener.RecordListener;

public final class AsyncOperateRead extends AsyncRead {
	private final OperateArgs args;

	public AsyncOperateRead(Cluster cluster, RecordListener listener, Key key, OperateArgs args) {
		super(cluster, listener, args.writePolicy, key, true);
		this.args = args;
	}

	@Override
	protected void writeBuffer() {
		setOperate(args.writePolicy, key, args);
	}
}
