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
package com.aerospike.benchmarks;

import java.io.PrintStream;

public interface LatencyManager {

	public void add(long elapsed);

	public void printHeader(PrintStream stream);

	/**
	 * Print latency percents for specified cumulative ranges.
	 * This function is not absolutely accurate for a given time slice because this method
	 * is not synchronized with the add() method.  Some values will slip into the next iteration.
	 * It is not a good idea to add extra locks just to measure performance since that actually
	 * affects performance.  Fortunately, the values will even out over time
	 * (ie. no double counting).
	 */
	public void printResults(PrintStream stream, String prefix);

	public void printSummaryHeader(PrintStream stream);

	public void printSummary(PrintStream stream, String prefix);
}
