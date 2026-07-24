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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.util.Version;

public final class ExampleServerRequirement {
	public static final ExampleServerRequirement NONE = new ExampleServerRequirement(null, false, false, false);

	private final Version minimumServerVersion;
	private final boolean requiresEnterpriseEdition;
	private final boolean requiresStrongConsistencyNamespace;
	private final boolean requiresTtlSupport;

	private ExampleServerRequirement(
		Version minimumServerVersion,
		boolean requiresEnterpriseEdition,
		boolean requiresStrongConsistencyNamespace,
		boolean requiresTtlSupport
	) {
		this.minimumServerVersion = minimumServerVersion;
		this.requiresEnterpriseEdition = requiresEnterpriseEdition;
		this.requiresStrongConsistencyNamespace = requiresStrongConsistencyNamespace;
		this.requiresTtlSupport = requiresTtlSupport;
	}

	public static ExampleServerRequirement minimumServerVersion(int major, int minor, int patch) {
		return new ExampleServerRequirement(new Version(major, minor, patch, 0), false, false, false);
	}

	public static ExampleServerRequirement minimumServerVersion(Version minimumServerVersion) {
		return new ExampleServerRequirement(minimumServerVersion, false, false, false);
	}

	public static ExampleServerRequirement enterpriseEdition() {
		return new ExampleServerRequirement(null, true, false, false);
	}

	public static ExampleServerRequirement strongConsistencyNamespace() {
		return new ExampleServerRequirement(null, false, true, false);
	}

	public static ExampleServerRequirement ttlSupported() {
		return new ExampleServerRequirement(null, false, false, true);
	}

	public static ExampleServerRequirement enterpriseEditionAndMinimumServerVersion(
		int major,
		int minor,
		int patch
	) {
		return new ExampleServerRequirement(new Version(major, minor, patch, 0), true, false, false);
	}

	public ExampleServerRequirement andEnterpriseEdition() {
		return new ExampleServerRequirement(
			minimumServerVersion,
			true,
			requiresStrongConsistencyNamespace,
			requiresTtlSupport);
	}

	public ExampleServerRequirement andStrongConsistencyNamespace() {
		return new ExampleServerRequirement(
			minimumServerVersion,
			requiresEnterpriseEdition,
			true,
			requiresTtlSupport);
	}

	public ExampleServerRequirement andTtlSupport() {
		return new ExampleServerRequirement(
			minimumServerVersion,
			requiresEnterpriseEdition,
			requiresStrongConsistencyNamespace,
			true);
	}

	public Version minimumServerVersion() {
		return minimumServerVersion;
	}

	public boolean requiresEnterpriseEdition() {
		return requiresEnterpriseEdition;
	}

	public boolean requiresStrongConsistencyNamespace() {
		return requiresStrongConsistencyNamespace;
	}

	public boolean requiresTtlSupport() {
		return requiresTtlSupport;
	}

	public boolean hasRequirements() {
		return minimumServerVersion != null ||
			requiresEnterpriseEdition ||
			requiresStrongConsistencyNamespace ||
			requiresTtlSupport;
	}

	public String unmetReason(
		Version serverVersion,
		boolean enterpriseEdition,
		boolean strongConsistencyNamespace,
		boolean ttlSupported
	) {
		List<String> unmet = new ArrayList<String>(4);

		if (requiresEnterpriseEdition && ! enterpriseEdition) {
			unmet.add("requires Aerospike Enterprise Edition");
		}

		if (requiresStrongConsistencyNamespace && ! strongConsistencyNamespace) {
			unmet.add("requires a strong-consistency namespace");
		}

		if (requiresTtlSupport && ! ttlSupported) {
			unmet.add("requires TTL support in the target namespace");
		}

		if (minimumServerVersion != null && ! serverVersion.isGreaterOrEqual(minimumServerVersion)) {
			unmet.add("requires server version " + formatVersion(minimumServerVersion) + " or later");
		}

		if (unmet.isEmpty()) {
			return null;
		}
		if (unmet.size() == 1) {
			return unmet.get(0);
		}
		return unmet.get(0) + " and " + unmet.get(1);
	}

	private static String formatVersion(Version version) {
		String formatted = version.toString();
		return formatted.endsWith(".0") ? formatted.substring(0, formatted.length() - 2) : formatted;
	}
}
