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
package com.aerospike.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.aerospike.test.discovery.TestK8sPassThrough;

/**
 * Runs the discovery tests against a kind cluster running the Aerospike Kubernetes Operator, built
 * by .github/workflows/discovery-k8s.yaml or test/discovery/run_l3.sh. Without that cluster
 * (DISCOVERY_KUBECONFIG unset) every test is skipped.
 *
 * <p>The suite builds no shared client: the seed is reachable only through the endpoint and its
 * peers only through what the server advertises, so each test builds its own client.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestK8sPassThrough.class
})
public class SuiteDiscoveryK8s {
}
