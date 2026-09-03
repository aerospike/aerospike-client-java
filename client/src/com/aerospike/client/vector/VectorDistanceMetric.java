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
package com.aerospike.client.vector;

/**
 * Distance metric used to compare two vectors in a vector distance expression.
 * See {@link com.aerospike.client.exp.VectorExp#distance}.
 */
public enum VectorDistanceMetric {
	/**
	 * Squared Euclidean (L2) distance. Smaller values indicate closer vectors.
	 */
	EUCLIDEAN(0),

	/**
	 * Dot product. Larger values indicate more similar vectors.
	 */
	DOT_PRODUCT(1),

	/**
	 * Cosine similarity. Larger values indicate closer (more similar) vectors.
	 */
	COSINE(2);

	private final int code;

	VectorDistanceMetric(final int code) {
		this.code = code;
	}

	/**
	 * Return the internal metric identifier.
	 */
	public int getCode() {
		return code;
	}
}
