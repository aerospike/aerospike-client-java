/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0.
 */
package com.aerospike.client.metrics;

/**
 * Selects where metrics snapshots are exported.
 */
public enum MetricsExporterType {
	/**
	 * Do not export periodic snapshots.
	 */
	NONE,

	/**
	 * Export snapshots with the client-owned {@link MetricsWriter}.
	 */
	FILE,

	/**
	 * Export snapshots to application-registered exporters.
	 */
	CUSTOM;

	/**
	 * Parse a case-insensitive exporter type.
	 */
	public static MetricsExporterType parse(String value) {
		if (value == null) {
			return null;
		}
		return valueOf(value.trim().toUpperCase());
	}
}
