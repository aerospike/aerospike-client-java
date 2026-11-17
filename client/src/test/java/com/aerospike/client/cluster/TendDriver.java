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
package com.aerospike.client.cluster;

import static org.junit.Assert.assertFalse;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Drive cluster tends from the test thread.
 */
final class TendDriver {
	/**
	 * Stop the tend thread so the test drives every later tend.
	 */
	static void stopTendThread(Cluster cluster) throws Exception {
		Field field = Cluster.class.getDeclaredField("tendThread");
		field.setAccessible(true);
		Thread thread = (Thread)field.get(cluster);

		cluster.tendValid = false;
		thread.interrupt();
		thread.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse("tend thread did not stop", thread.isAlive());
	}

	static void tend(Cluster cluster) throws Exception {
		Method method = Cluster.class.getDeclaredMethod("tend", boolean.class, boolean.class);
		method.setAccessible(true);

		try {
			method.invoke(cluster, false, false);
		}
		catch (InvocationTargetException ite) {
			throw (Exception)ite.getCause();
		}
	}

	/**
	 * Run one tend and return how long it took in milliseconds.
	 */
	static long timedTend(Cluster cluster) throws Exception {
		long begin = System.nanoTime();
		tend(cluster);
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
	}

	private TendDriver() {
	}
}
