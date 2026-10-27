/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.discovery;

import java.util.HashMap;
import java.util.Map;

/**
 * Environment seam pointing the AWS credential chain at a {@link StubAwsServer}, with a clock offset.
 */
final class TestAwsEnvironment extends AwsEnvironment {
	final Map<String,String> vars = new HashMap<>();
	final String endpoint;
	String home = "/nonexistent-home";
	volatile long clockOffset;

	TestAwsEnvironment(String endpoint) {
		this.endpoint = endpoint;
	}

	TestAwsEnvironment set(String name, String value) {
		vars.put(name, value);
		return this;
	}

	@Override
	String getenv(String name) {
		return vars.get(name);
	}

	@Override
	String userHome() {
		return home;
	}

	@Override
	String ec2Endpoint(String region) {
		return endpoint;
	}

	@Override
	String stsEndpoint(String region) {
		return endpoint + "/sts";
	}

	@Override
	String ecsEndpoint() {
		return endpoint + "/ecs";
	}

	@Override
	String imdsEndpoint() {
		return endpoint;
	}

	@Override
	long currentTimeMillis() {
		return System.currentTimeMillis() + clockOffset;
	}
}
