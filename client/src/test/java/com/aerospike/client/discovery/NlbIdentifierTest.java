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
package com.aerospike.client.discovery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;

import com.aerospike.client.configuration.serializers.staticconfig.StaticDiscoveryConfig;
import com.aerospike.client.configuration.serializers.staticconfig.StaticTranslationConfig;
import com.aerospike.client.policy.ClientPolicy;

/**
 * U-11: no public discovery or translation identifier matches {@code /nlb/i}.
 */
public class NlbIdentifierTest {
	private static final Pattern NLB = Pattern.compile("nlb", Pattern.CASE_INSENSITIVE);
	private static final Pattern YAML_KEY = Pattern.compile("[a-z][a-z0-9_.-]*");

	@Test
	public void patternMatchesAnyCase() {
		assertTrue(NLB.matcher("awsNlbIdle").find());
		assertTrue(NLB.matcher("NLB").find());
		assertTrue(NLB.matcher("idle_nlb_ms").find());
	}

	@Test
	public void discoveryPackageHasNoNlbIdentifier() throws Exception {
		List<String> ids = new ArrayList<>();

		for (Class<?> cls : discoveryTypes()) {
			ids.add(cls.getSimpleName());
			addMembers(cls, ids);
		}

		assertTrue(ids.toString(), ids.containsAll(List.of("AddressTranslator", "SeedCandidateProvider",
			"SourceList", "ALTERNATE", "translate", "PassThroughAddressTranslator", "INSTANCE")));
		assertNone(ids);
	}

	@Test
	public void clientPolicyHasNoNlbIdentifier() {
		List<String> ids = new ArrayList<>();
		addMembers(ClientPolicy.class, ids);

		assertTrue(ids.toString(), ids.containsAll(List.of("ipMap", "addressTranslator", "translationOnMiss",
			"seedCandidateProvider", "discoveryMergePolicy", "discoveryRefreshInterval",
			"discoveryTendRefreshDeadline", "useServicesAlternate", "setAddressTranslator", "setSeedCandidateProvider")));
		assertNone(ids);
	}

	@Test
	public void yamlKeysHaveNoNlbIdentifier() throws Exception {
		List<String> ids = new ArrayList<>();

		for (Class<?> cls : new Class<?>[] {StaticDiscoveryConfig.class, StaticTranslationConfig.class}) {
			addMembers(cls, ids);

			for (String s : new ClassConstants(cls).strings) {
				if (YAML_KEY.matcher(s).matches()) {
					ids.add(s);
				}
			}
		}

		assertTrue(ids.toString(), ids.containsAll(List.of("provider", "merge_policy", "refresh_interval_ms",
			"tend_refresh_deadline_ms", "names", "record_type", "port", "tls_name", "timeout_ms", "servers",
			"on_miss", "static_map", "port_map", "static-map", "port-map", "pass-through", "pass_through")));
		assertNone(ids);
	}

	private static List<Class<?>> discoveryTypes() throws Exception {
		Path dir = Paths.get(AddressTranslator.class.getResource("AddressTranslator.class").toURI()).getParent();
		String pkg = AddressTranslator.class.getPackageName();
		List<Class<?>> types = new ArrayList<>();

		try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.class")) {
			for (Path file : files) {
				String name = file.getFileName().toString();
				Class<?> cls = Class.forName(pkg + '.' + name.substring(0, name.length() - 6), false,
					AddressTranslator.class.getClassLoader());

				if (Modifier.isPublic(cls.getModifiers())) {
					types.add(cls);
				}
			}
		}
		return types;
	}

	private static void addMembers(Class<?> cls, List<String> ids) {
		for (Field field : cls.getDeclaredFields()) {
			if (Modifier.isPublic(field.getModifiers())) {
				ids.add(field.getName());
			}
		}

		for (Method method : cls.getDeclaredMethods()) {
			if (Modifier.isPublic(method.getModifiers()) && ! method.isSynthetic()) {
				ids.add(method.getName());
			}
		}
	}

	private static void assertNone(List<String> ids) {
		for (String id : ids) {
			assertFalse(id, NLB.matcher(id).find());
		}
	}
}
