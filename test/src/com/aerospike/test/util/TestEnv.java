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
package com.aerospike.test.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Values of an aerospike-client-conformance test environment (envs/&lt;environment&gt;.sh up), read
 * from the environment variables its values file sets, and its control verbs, run as
 * {@code "$ENV_CTL" <verb>}.
 */
public final class TestEnv {
	private static final int CONTROL_WAIT_SECONDS = 300;

	private TestEnv() {
	}

	/**
	 * Return the value of a key, failing when it is not set or empty.
	 */
	public static String get(String key) {
		String value = System.getenv(key);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("Environment variable " + key + " is not set. Bring the conformance " +
				"environment up with envs/<environment>.sh up and load its values file");
		}
		return value;
	}

	/**
	 * Return the integer value of a key, failing when it is not set.
	 */
	public static int getInt(String key) {
		return Integer.parseInt(get(key));
	}

	/**
	 * Return the value of a key that may be empty, failing when it is not set at all.
	 */
	public static String getOrEmpty(String key) {
		String value = System.getenv(key);

		if (value == null) {
			get(key);
		}
		return value;
	}

	/**
	 * Run a verb of the environment's script and return its standard output, failing when it does
	 * not exit with 0.
	 */
	public static String control(String... verb) throws Exception {
		List<String> command = new ArrayList<>();
		command.add(get("ENV_CTL"));
		command.addAll(Arrays.asList(verb));
		Process process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();

		if (! process.waitFor(CONTROL_WAIT_SECONDS, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IllegalStateException(command + " did not finish within " + CONTROL_WAIT_SECONDS + "s");
		}

		if (process.exitValue() != 0) {
			throw new IllegalStateException(command + " exited with " + process.exitValue() + ": " + output);
		}
		return output;
	}
}
