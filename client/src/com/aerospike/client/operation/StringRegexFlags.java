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
package com.aerospike.client.operation;

/**
 * Regex flags for {@link StringOperation#regexCompare} and
 * {@link StringOperation#regexReplace}. Combine with bitwise OR.
 */
public final class StringRegexFlags {
	/**
	 * Default. No flags set.
	 */
	public static final int DEFAULT = 0;

	/**
	 * Case insensitive matching.
	 */
	public static final int CASE_INSENSITIVE = 1 << 0;

	/**
	 * Treat input as a multi-line string. {@code ^} and {@code $} match
	 * the start and end of any line, not just the start and end of the input.
	 */
	public static final int MULTILINE = 1 << 1;

	/**
	 * The {@code .} metacharacter matches any character including line terminators.
	 */
	public static final int DOTALL = 1 << 2;

	/**
	 * Treat only {@code \n} as a line terminator (Unix-style line endings).
	 */
	public static final int UNIX_LINES = 1 << 3;

	/**
	 * Replace all matches in the input. Only applicable to
	 * {@link StringOperation#regexReplace}.
	 */
	public static final int GLOBAL = 1 << 4;
}
