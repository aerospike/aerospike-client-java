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
package com.aerospike.client.cluster;

import java.util.concurrent.atomic.AtomicReferenceArray;

public final class Partitions {
	public final AtomicReferenceArray<Node>[] replicas;
	final int[] regimes;
	public final boolean scMode;

	@SuppressWarnings("unchecked")
	public Partitions(int partitionCount, int replicaCount, boolean scMode) {
		this.replicas = new AtomicReferenceArray[replicaCount];

		for (int i = 0; i < replicaCount; i++) {
			this.replicas[i] = new AtomicReferenceArray<Node>(partitionCount);
		}
		this.regimes = new int[partitionCount];
		this.scMode = scMode;
	}

	/**
	 * Copy partition map while reserving space for a new replica count.
	 */
	@SuppressWarnings("unchecked")
	public Partitions(Partitions other, int replicaCount) {
		this.replicas = new AtomicReferenceArray[replicaCount];

		if (other.replicas.length < replicaCount) {
			int i = 0;

			// Copy existing entries.
			for (; i < other.replicas.length; i++) {
				this.replicas[i] = other.replicas[i];
			}

			// Create new entries.
			for (; i < replicaCount; i++) {
				this.replicas[i] = new AtomicReferenceArray<Node>(other.regimes.length);
			}
		}
		else {
			// Copy existing entries.
			for (int i = 0; i < replicaCount; i++) {
				this.replicas[i] = other.replicas[i];
			}
		}
		this.regimes = other.regimes;
		this.scMode = other.scMode;
	}
}
