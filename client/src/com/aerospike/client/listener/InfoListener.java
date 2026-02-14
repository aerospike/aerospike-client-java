/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.listener;

import java.util.Map;

import com.aerospike.client.AerospikeException;

/**
 * Async callback for info command; receives a map of info key-value pairs or exception.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#info}
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * Node node = client.getNodes()[0];
 *
 * client.info(loop, new InfoListener() {
 *   public void onSuccess(Map map) { }
 *   public void onFailure(AerospikeException e) { }
 * }, new InfoPolicy(), node, "build");
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#info
 */
public interface InfoListener {
	/**
	 * Called when the info command completes successfully.
	 * @param map map of info keys to result values (must not be null)
	 */
	public void onSuccess(Map<String,String> map);

	/**
	 * Called when the info command fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}
