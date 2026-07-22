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
package com.aerospike.client.exp;

import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.VectorDistanceMetric;

/**
 * Vector expression generator. See {@link com.aerospike.client.exp.Exp}.
 */
public final class VectorExp {
	/**
	 * Create expression that returns the distance between a stored vector bin and a query
	 * vector as a 64 bit float, using the given distance metric.
	 * <p>
	 * The query vector's element type and dimension count must match the stored vector.
	 * If the bin does not hold a vector, or its dimensions do not match the query, the
	 * expression evaluates to unknown.
	 *
	 * <pre>{@code
	 * // Records whose "embedding" vector is within cosine distance threshold of a query
	 * Vector query = Vector.ofFloat32(queryEmbedding);
	 * Exp.gt(
	 *     VectorExp.distance(VectorDistanceMetric.COSINE, query, Exp.vectorBin("embedding")),
	 *     Exp.val(0.8))
	 * }</pre>
	 *
	 * @param metric	distance metric used to compare the vectors
	 * @param query		query vector compared against the stored vector bin
	 * @param bin		vector bin read, typically {@link Exp#vectorBin(String)}
	 */
	public static Exp distance(VectorDistanceMetric metric, Vector query, Exp bin) {
		return new Exp.VectorDist(metric.getCode(), query.getElementBytes(), bin);
	}

	private VectorExp() {
	}
}
