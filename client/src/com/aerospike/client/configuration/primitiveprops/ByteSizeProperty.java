/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.configuration.primitiveprops;

/**
 * Configuration byte size. Supported suffixes are B, KiB, MiB and GiB.
 */
public class ByteSizeProperty {
	/** Parsed byte count. */
	public long value;

	/** Initialize an empty property for YAML deserialization. */
	public ByteSizeProperty() {
	}

	/** Initialize a property with a byte count. */
	public ByteSizeProperty(long value) {
		this.value = value;
	}

	/** Initialize a property from a binary byte-size string. */
	public ByteSizeProperty(String value) {
		this.value = parse(value);
	}

	/** Set the value from a binary byte-size string. */
	public void setValue(String value) {
		this.value = parse(value);
	}

	/** Set the value as a byte count. */
	public void setValue(long value) {
		this.value = value;
	}

	/**
	 * Parse a byte count using B, KiB, MiB or GiB binary units.
	 */
	public static long parse(String value) {
		String text = value.trim().toUpperCase();
		long multiplier = 1;
		String number = text;

		if (text.endsWith("KIB")) {
			multiplier = 1024L;
			number = text.substring(0, text.length() - 3);
		}
		else if (text.endsWith("MIB")) {
			multiplier = 1024L * 1024L;
			number = text.substring(0, text.length() - 3);
		}
		else if (text.endsWith("GIB")) {
			multiplier = 1024L * 1024L * 1024L;
			number = text.substring(0, text.length() - 3);
		}
		else if (text.endsWith("B")) {
			number = text.substring(0, text.length() - 1);
		}
		return Math.multiplyExact(Long.parseLong(number.trim()), multiplier);
	}
}
