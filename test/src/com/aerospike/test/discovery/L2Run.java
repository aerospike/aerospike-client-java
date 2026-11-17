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
package com.aerospike.test.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * Runs the L2 test classes named on the command line for the l2-proxy harness of
 * aerospike-client-conformance, which runs test/discovery/l2_run.sh as the manifest's L2_RUN. Exits
 * non-zero when a test fails, when no test runs, or when a test skips that the comma-separated
 * expected skips do not name as {@code <simple class name>.<method>} or {@code <simple class name>.*}.
 * The harness passes on the exit code alone, so a profile whose environment is missing would
 * otherwise pass with every test skipped.
 *
 * <p>Usage: {@code L2Run <expected skips> <test class> ...}
 */
public final class L2Run {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: L2Run <expected skips> <test class> ...");
			System.exit(2);
		}

		Set<String> expected = new HashSet<>(Arrays.asList(args[0].split(",")));
		expected.remove("");
		Class<?>[] classes = new Class<?>[args.length - 1];

		for (int i = 1; i < args.length; i++) {
			classes[i - 1] = Class.forName(args[i]);
		}

		List<String> skipped = new ArrayList<>();
		JUnitCore core = new JUnitCore();
		core.addListener(new RunListener() {
			@Override
			public void testStarted(Description description) {
				System.out.println("L2-TEST start " + name(description));
			}

			@Override
			public void testFailure(Failure failure) {
				System.out.println("L2-TEST fail " + name(failure.getDescription()));
				System.out.println(failure.getTrace());
			}

			@Override
			public void testAssumptionFailure(Failure failure) {
				skipped.add(name(failure.getDescription()));
				System.out.println("L2-TEST skip " + name(failure.getDescription()) + ": " + failure.getMessage());
			}

			@Override
			public void testIgnored(Description description) {
				skipped.add(name(description));
				System.out.println("L2-TEST skip " + name(description));
			}
		});

		Result result = core.run(classes);
		List<String> unexpected = new ArrayList<>();

		for (String name : skipped) {
			String cls = name.substring(0, name.indexOf('.'));

			if (! expected.contains(name) && ! expected.contains(cls + ".*")) {
				unexpected.add(name);
			}
		}

		System.out.println("L2-RESULT run=" + result.getRunCount() + " failed=" + result.getFailureCount() +
			" skipped=" + skipped.size() + " unexpectedSkips=" + unexpected);

		if (result.getRunCount() == 0 || result.getFailureCount() > 0 || ! unexpected.isEmpty()) {
			System.exit(1);
		}
		System.exit(0);
	}

	private static String name(Description description) {
		String cls = description.getTestClass() != null ? description.getTestClass().getSimpleName() :
			description.getClassName().substring(description.getClassName().lastIndexOf('.') + 1);
		String method = description.getMethodName();
		return cls + '.' + (method != null ? method : "*");
	}
}
