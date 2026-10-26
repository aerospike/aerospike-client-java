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

import java.time.Duration;

/**
 * Configuration duration. Supported suffixes are ms, s, m and h.
 */
public class DurationProperty {
	/** Parsed duration. */
	public Duration value;

	/** Initialize an empty property for YAML deserialization. */
	public DurationProperty() {
	}

	/** Initialize a property with a duration. */
	public DurationProperty(Duration value) {
		this.value = value;
	}

	/** Initialize a property from a duration string. */
	public DurationProperty(String value) {
		this.value = parse(value);
	}

	/** Set the value from a duration string. */
	public void setValue(String value) {
		this.value = parse(value);
	}

	/**
	 * Parse a duration using ms, s, m or h units.
	 */
	public static Duration parse(String value) {
		String text = value.trim().toLowerCase();
		int suffixLength = text.endsWith("ms") ? 2 : 1;
		long amount = Long.parseLong(text.substring(0, text.length() - suffixLength));

		if (text.endsWith("ms")) {
			return Duration.ofMillis(amount);
		}
		if (text.endsWith("s")) {
			return Duration.ofSeconds(amount);
		}
		if (text.endsWith("m")) {
			return Duration.ofMinutes(amount);
		}
		if (text.endsWith("h")) {
			return Duration.ofHours(amount);
		}
		throw new IllegalArgumentException("Invalid duration: " + value);
	}
}
