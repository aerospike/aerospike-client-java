/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.benchmarks;

public class Constants {
	public static final String ASBENCH_COMMAND = "run_benchmarks";
	public static final String CONNECTION_OPTIONS_HEADING = "Connection Options:";
	public static final String WORKLOAD_OPTIONS_HEADING = "Workload Options:";
	public static final String BENCHMARK_OPTIONS_HEADING = "Benchmark Options:";
	public static final String HELP_OPTIONS_HEADING = "Help Options:";

	public static final String USAGE_MESSAGE =
		"Aerospike benchmark utility to generate synthetic load against the Aerospike database.";
	public static final String SUCCESS_COMPLETION_MESSAGE =
		"Aerospike Benchmark completed successfully.";
	public static final String ERROR_COMPLETION_MESSAGE = "Aerospike Benchmark execution failed.";
	public static final String INVALID_PORT_MESSAGE =
		"Invalid value '%d' for option '--port': value must be between 1 and 65535";
	public static final String INVALID_AUTH_MODE_MESSAGE =
		"Invalid value '%s' for option '--authMode': value must be one of %s";
	public static final String INVALID_THREADS_MESSAGE =
		"Client threads (-z) must be > 0 (found: %d)";
	public static final String INVALID_VIRTUAL_THREADS_MESSAGE =
		"Virtual threads (-vt) must be > 0 (found: %d)";
	public static final String INVALID_EVENT_LOOP_TYPE_MESSAGE =
		"Invalid eventLoopType: '%s'. Allowed values are: %s";
	public static final String INVALID_EXPIRATION_TIME_MESSAGE =
		"Expiration time must be >= -1 (found: %d)";
	public static final String INVALID_READ_TOUCH_TTL_PERCENT_MESSAGE =
		"readTouchTtlPercent must be between 0 and 100 (found: %d)";
	public static final String INVALID_READ_MODE_AP_MESSAGE =
		"Invalid readModeAP: '%s'. Allowed values are: one, all";
	public static final String INVALID_READ_MODE_SC_MESSAGE =
		"Invalid readModeSC: '%s'. Allowed values are: session, linearize, allow_replica,"
			+ " allow_unavailable";
	public static final String INVALID_COMMIT_LEVEL_MESSAGE =
		"Invalid commitLevel: '%s'. Allowed values are: all, master";
}