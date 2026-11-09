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

/**
 * Environment read by the AWS credential chain and the default AWS endpoints. Tests override it to
 * point the chain at stubs; production reads the process environment.
 */
class AwsEnvironment {
	static final AwsEnvironment SYSTEM = new AwsEnvironment();

	String getenv(String name) {
		return System.getenv(name);
	}

	String userHome() {
		return System.getProperty("user.home");
	}

	String ec2Endpoint(String region) {
		return "https://ec2." + region + '.' + domain(region);
	}

	String stsEndpoint(String region) {
		return "https://sts." + region + '.' + domain(region);
	}

	String ecsEndpoint() {
		return "http://169.254.170.2";
	}

	String imdsEndpoint() {
		return "http://169.254.169.254";
	}

	long currentTimeMillis() {
		return System.currentTimeMillis();
	}

	private static String domain(String region) {
		return region.startsWith("cn-")? "amazonaws.com.cn" : "amazonaws.com";
	}
}
