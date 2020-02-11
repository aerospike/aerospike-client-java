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
package com.aerospike.client.query;

/**
 * Regex bit flags.
 */
public final class RegexFlag {
	/**
	 * Use regex defaults.
	 */
	public static final int NONE = 0;

	/**
	 * Use POSIX Extended Regular Expression syntax when interpreting regex.
	 */
	public static final int EXTENDED = 1;

	/**
	 * Do not differentiate case.
	 */
	public static final int ICASE = 2;

	/**
	 * Do not report position of matches.
	 */
	public static final int NOSUB = 4;

	/**
	 * Match-any-character operators don't match a newline.
	 */
	public static final int NEWLINE = 8;
}
