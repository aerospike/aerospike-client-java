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
package com.aerospike.client.metrics;

/**
 * Immutable approved P1/P2 usage metrics.
 * Values remain zero until the corresponding collection work is implemented.
 */
public final class UsageMetricsSnapshot {
	/** Cumulative blocking public API calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long blocking;

	/** Cumulative deferred public API calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long deferred;

	/** Cumulative background public API calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long background;

	/** Cumulative single-key point-operation calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long shapePoint;

	/** Cumulative multi-key batch calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long shapeBatch;

	/** Cumulative query or scan calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long shapeQuery;

	/** Cumulative object-mapping read calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long objectMappingRead;

	/** Cumulative object-mapping write calls. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long objectMappingWrite;

	/** Cumulative calls using an AEL record filter. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long filterAel;

	/** Cumulative calls using a legacy expression record filter. */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long filterExp;

	/**
	 * Initialize an immutable usage metrics snapshot.
	 */
	public UsageMetricsSnapshot(
		long blocking,
		long deferred,
		long background,
		long shapePoint,
		long shapeBatch,
		long shapeQuery,
		long objectMappingRead,
		long objectMappingWrite,
		long filterAel,
		long filterExp
	) {
		this.blocking = blocking;
		this.deferred = deferred;
		this.background = background;
		this.shapePoint = shapePoint;
		this.shapeBatch = shapeBatch;
		this.shapeQuery = shapeQuery;
		this.objectMappingRead = objectMappingRead;
		this.objectMappingWrite = objectMappingWrite;
		this.filterAel = filterAel;
		this.filterExp = filterExp;
	}

	@Override
	public String toString() {
		return "UsageMetricsSnapshot{"
			+ "blocking=" + blocking
			+ ", deferred=" + deferred
			+ ", background=" + background
			+ ", shapePoint=" + shapePoint
			+ ", shapeBatch=" + shapeBatch
			+ ", shapeQuery=" + shapeQuery
			+ ", objectMappingRead=" + objectMappingRead
			+ ", objectMappingWrite=" + objectMappingWrite
			+ ", filterAel=" + filterAel
			+ ", filterExp=" + filterExp
			+ '}';
	}
}
