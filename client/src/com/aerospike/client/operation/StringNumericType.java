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
 * Numeric type filter for {@link StringOperation#isNumeric}.
 */
public final class StringNumericType {
	/**
	 * Match either an integer or a floating-point number.
	 */
	public static final int ANY = 0;

	/**
	 * Match only integers.
	 */
	public static final int INT = 1;

	/**
	 * Match only floating-point numbers. Stricter than "parses as a float": the
	 * string must contain a {@code '.'} followed by a digit, so {@code "5"} is
	 * false under {@code FLOAT} but true under {@link #ANY}.
	 */
	public static final int FLOAT = 2;
}
