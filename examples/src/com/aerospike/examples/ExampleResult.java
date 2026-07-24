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
package com.aerospike.examples;

public final class ExampleResult {
	private final String name;
	private final ExampleStatus status;
	private final long elapsedMillis;
	private final Throwable error;
	private final String message;

	private ExampleResult(
		String name,
		ExampleStatus status,
		long elapsedMillis,
		Throwable error,
		String message
	) {
		this.name = name;
		this.status = status;
		this.elapsedMillis = elapsedMillis;
		this.error = error;
		this.message = message;
	}

	public static ExampleResult passed(String name, long elapsedMillis) {
		return new ExampleResult(name, ExampleStatus.PASSED, elapsedMillis, null, null);
	}

	public static ExampleResult failed(String name, long elapsedMillis, Throwable error) {
		return new ExampleResult(name, ExampleStatus.FAILED, elapsedMillis, error, null);
	}

	public static ExampleResult skipped(String name, long elapsedMillis, String message) {
		return new ExampleResult(name, ExampleStatus.SKIPPED, elapsedMillis, null, message);
	}

	public String name() {
		return name;
	}

	public ExampleStatus status() {
		return status;
	}

	public long elapsedMillis() {
		return elapsedMillis;
	}

	public Throwable error() {
		return error;
	}

	public String message() {
		return message;
	}

	public boolean failed() {
		return status == ExampleStatus.FAILED;
	}
}
