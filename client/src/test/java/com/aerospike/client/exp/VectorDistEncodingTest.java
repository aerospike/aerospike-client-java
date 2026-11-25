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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.VectorDistanceMetric;

/**
 * Vector distance packs as {@code [52, metric, query, bin]} for every metric.
 * No server required.
 */
public class VectorDistEncodingTest {

	@Test
	public void distancePacksOneOpWithMetricArgument() {
		Vector query = Vector.ofFloat32(new float[] {0.1f, 0.2f});
		Expression[] exps = {
			Exp.build(VectorExp.distance(VectorDistanceMetric.EUCLIDEAN_SQUARED, query, Exp.vectorBin("v"))),
			Exp.build(VectorExp.distance(VectorDistanceMetric.DOT_PRODUCT, query, Exp.vectorBin("v"))),
			Exp.build(VectorExp.distance(VectorDistanceMetric.COSINE_SIMILARITY, query, Exp.vectorBin("v")))
		};
		int[] metrics = {
			VectorDistanceMetric.EUCLIDEAN_SQUARED.getCode(),
			VectorDistanceMetric.DOT_PRODUCT.getCode(),
			VectorDistanceMetric.COSINE_SIMILARITY.getCode()
		};

		for (int i = 0; i < exps.length; i++) {
			byte[] packed = exps[i].getBytes();
			// Fixarray of 4, then op 52, then metric integer (0..2 fit in a positive fixint).
			assertEquals("array header", (byte)(0x90 | 4), packed[0]);
			assertEquals("VECTOR_DISTANCE op", 52, packed[1] & 0xff);
			assertEquals("metric", metrics[i], packed[2] & 0xff);
			assertTrue(exps[i].hasVector());
		}

		// Everything after the metric is identical across metrics.
		byte[] euclidean = exps[0].getBytes();
		for (int i = 1; i < exps.length; i++) {
			byte[] other = exps[i].getBytes();
			assertEquals(euclidean.length, other.length);
			assertArrayEquals(
				java.util.Arrays.copyOfRange(euclidean, 3, euclidean.length),
				java.util.Arrays.copyOfRange(other, 3, other.length));
		}
	}
}
