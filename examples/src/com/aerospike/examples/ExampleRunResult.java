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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ExampleRunResult {
	private final List<ExampleResult> results;

	public ExampleRunResult(List<ExampleResult> results) {
		this.results = Collections.unmodifiableList(new ArrayList<ExampleResult>(results));
	}

	public List<ExampleResult> results() {
		return results;
	}

	public boolean hasFailures() {
		return count(ExampleStatus.FAILED) > 0;
	}

	public int passedCount() {
		return count(ExampleStatus.PASSED);
	}

	public int failedCount() {
		return count(ExampleStatus.FAILED);
	}

	public int skippedCount() {
		return count(ExampleStatus.SKIPPED);
	}

	public ExampleRunResult append(ExampleRunResult other) {
		if (other == null || other.results.isEmpty()) {
			return this;
		}

		List<ExampleResult> combined = new ArrayList<ExampleResult>(results.size() + other.results.size());
		combined.addAll(results);
		combined.addAll(other.results);
		return new ExampleRunResult(combined);
	}

	private int count(ExampleStatus status) {
		int count = 0;

		for (ExampleResult result : results) {
			if (result.status() == status) {
				count++;
			}
		}
		return count;
	}
}
