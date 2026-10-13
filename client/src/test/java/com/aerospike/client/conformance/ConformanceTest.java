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
package com.aerospike.client.conformance;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.x500.X500Principal;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.ConformanceAdapter;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

/**
 * Run the shared L1 conformance fixtures against the client and compare each row's result with
 * the result Java declares for it.
 */
@RunWith(Parameterized.class)
public class ConformanceTest {
	private static final String PASS = "pass";
	private static final String FAIL = "fail";
	private static final String UNSUPPORTED = "unsupported";
	private static final String BLOCKED = "blocked";
	private static final Set<String> RESULTS = Set.of(PASS, FAIL, UNSUPPORTED, BLOCKED);

	private static final List<String> REQUIRED_FILES = List.of(
		"VERSION",
		"README.md",
		"schema/fixture.schema.json",
		"schema/report.schema.json",
		"discovery/translate/static-map.yaml",
		"discovery/translate/legacy-ip-map.yaml",
		"discovery/translate/port-map.yaml",
		"discovery/translate/endpoint-port-map.yaml",
		"discovery/translate/on-miss.yaml",
		"discovery/merge/merge-policy.yaml",
		"tls/name-matching.yaml",
		"tls/generate-certs.sh"
	);

	private static final Set<String> FILE_KEYS = Set.of("kind", "description", "shapes", "rows");
	private static final Set<String> ROW_KEYS = Set.of("case", "because", "state", "blockedBy",
		"config", "input", "expect");
	private static final String FIXTURE_SET_VERSION = "0.1.0";
	private static final String DECLARED = "/conformance/java-declared.yaml";
	private static final Path REPORT = Paths.get("target", "conformance-report.json");
	private static final String TLS_REJECT_PREFIX = "Invalid TLS name: ";
	private static final int MAX_PORT = 65535;
	private static final Pattern SHAPE_NAME = Pattern.compile("^[a-z0-9][a-z0-9-]*$");

	private static final List<Map<String,Object>> reportRows = Collections.synchronizedList(new ArrayList<>());
	private static Path dir;
	private static String fixtureSetVersion;

	@Parameters(name = "{0}")
	public static Collection<Object[]> data() throws Exception {
		String dirProperty = System.getProperty("conformance.dir");

		if (dirProperty == null || dirProperty.isEmpty()) {
			throw new IllegalStateException("Set -Dconformance.dir to an aerospike-client-conformance checkout or release");
		}
		dir = Paths.get(dirProperty).toAbsolutePath().normalize();

		if (! Files.isDirectory(dir)) {
			throw new IllegalStateException("Conformance directory not found: " + dir);
		}

		for (String name : REQUIRED_FILES) {
			if (! Files.isRegularFile(dir.resolve(name))) {
				throw new IllegalStateException("Conformance file not found: " + dir.resolve(name));
			}
		}

		fixtureSetVersion = Files.readString(dir.resolve("VERSION")).trim();

		if (! FIXTURE_SET_VERSION.equals(fixtureSetVersion)) {
			throw new IllegalStateException("Conformance fixture set " + fixtureSetVersion + " found, " +
				FIXTURE_SET_VERSION + " expected: check out that release or update the declarations");
		}

		List<Path> files;

		try (Stream<Path> stream = Files.walk(dir)) {
			files = stream
				.filter(p -> p.toString().endsWith(".yaml"))
				.filter(p -> ! dir.relativize(p).getName(0).toString().startsWith("."))
				.sorted()
				.collect(Collectors.toList());
		}

		Map<String,Map<String,Row>> rows = new LinkedHashMap<>();
		List<Object[]> params = new ArrayList<>();

		for (Path path : files) {
			String file = dir.relativize(path).toString().replace('\\', '/');
			Map<String,Row> fileRows = new LinkedHashMap<>();

			for (Row row : loadFile(path, file)) {
				fileRows.put(row.name, row);
				params.add(new Object[] {file + ":" + row.name, row});
			}
			rows.put(file, fileRows);
		}

		applyDeclarations(rows);
		return params;
	}

	private final Row row;

	public ConformanceTest(String name, Row row) {
		this.row = row;
	}

	@Test
	public void conformance() throws Exception {
		Outcome actual = execute();
		Declaration declared = row.declared;
		String reason = (declared.reason != null && declared.result.equals(actual.result))?
			declared.reason : actual.reason;

		Map<String,Object> entry = new LinkedHashMap<>();
		entry.put("file", row.file);
		entry.put("case", row.name);
		entry.put("result", actual.result);

		if (reason != null) {
			entry.put("reason", reason);
		}

		if (actual.reason != null && ! actual.reason.equals(reason)) {
			entry.put("detail", actual.reason);
		}
		reportRows.add(entry);

		assertEquals(row.file + ":" + row.name + " declared " + declared.result + ", actual " +
			actual.result + (actual.reason != null? ": " + actual.reason : ""), declared.result, actual.result);
	}

	@AfterClass
	public static void writeReport() throws Exception {
		Map<String,Integer> counts = new LinkedHashMap<>();

		for (String result : List.of(PASS, FAIL, UNSUPPORTED, BLOCKED)) {
			counts.put(result, 0);
		}

		List<Map<String,Object>> sorted = new ArrayList<>(reportRows);
		sorted.sort((a, b) -> (a.get("file") + ":" + a.get("case")).compareTo(b.get("file") + ":" + b.get("case")));

		for (Map<String,Object> entry : sorted) {
			counts.merge((String)entry.get("result"), 1, Integer::sum);
		}

		Map<String,Object> report = new LinkedHashMap<>();
		report.put("runtime", "java");
		report.put("clientVersion", clientVersion());
		report.put("fixtureSetVersion", fixtureSetVersion);
		report.put("counts", counts);
		report.put("rows", sorted);

		StringBuilder sb = new StringBuilder();
		writeJson(sb, report, "");
		sb.append('\n');
		Files.createDirectories(REPORT.getParent());
		Files.writeString(REPORT, sb.toString());
	}

	//-------------------------------------------------------
	// Execution
	//-------------------------------------------------------

	private Outcome execute() throws Exception {
		if (row.blockedBy != null) {
			return new Outcome(BLOCKED, row.blockedBy);
		}

		switch (row.kind) {
		case "translate":
			return translate();

		case "merge":
			return merge();

		default:
			return tls();
		}
	}

	private Outcome translate() {
		Map<String,Object> config = map(row.data.get("config"));
		Map<String,Object> input = map(row.data.get("input"));
		Map<String,Object> expect = map(row.data.get("expect"));
		Map<String,Object> translation = map(config.get("translation"));
		ClientPolicy policy = new ClientPolicy();

		if (translation == null) {
			policy.ipMap = stringMap(config.get("legacyHostMap"));
		}
		else {
			String provider = (String)translation.get("provider");

			if (! provider.equals("static-map")) {
				return new Outcome(UNSUPPORTED, "translator " + provider + " is not implemented");
			}

			if (translation.containsKey("onMiss")) {
				return new Outcome(UNSUPPORTED, "translation.onMiss is not implemented");
			}
			policy.addressTranslator = new StaticMapAddressTranslator(stringMap(translation.get("staticMap")));
		}

		if (expect.containsKey("error")) {
			return new Outcome(UNSUPPORTED, "translation-miss error is not implemented");
		}

		if (expect.containsKey("diagnostic")) {
			return new Outcome(UNSUPPORTED, "translation-miss diagnostic is not implemented");
		}

		Endpoint.SourceList sourceList = input.get("sourceList").equals("alternate")?
			Endpoint.SourceList.ALTERNATE : Endpoint.SourceList.STANDARD;
		Endpoint advertised = new Endpoint((String)input.get("nodeName"), (String)input.get("tlsName"),
			(String)input.get("host"), (Integer)input.get("port"), sourceList);
		Host actual;

		try {
			actual = ConformanceAdapter.translate(policy, advertised);
		}
		catch (Exception e) {
			return new Outcome(FAIL, "threw " + e);
		}

		String expected = endpoint((String)expect.get("host"), (Integer)expect.get("port"), (String)expect.get("tlsName"));
		String got = endpoint(actual.name, actual.port, actual.tlsName);

		if (! expected.equals(got)) {
			return new Outcome(FAIL, "expected " + expected + ", got " + got);
		}
		return new Outcome(PASS, null);
	}

	private Outcome merge() {
		Map<String,Object> config = map(row.data.get("config"));
		Map<String,Object> discovery = map(config.get("discovery"));
		ClientPolicy policy = new ClientPolicy();

		if (discovery != null && discovery.get("mergePolicy") != null) {
			policy.discoveryMergePolicy = mergePolicy((String)discovery.get("mergePolicy"));
		}

		List<Host[]> results = new ArrayList<>();

		for (Object result : list(map(row.data.get("input")).get("discoveryResults"))) {
			results.add(hosts(result));
		}

		List<Host[]> snapshots = ConformanceAdapter.refreshSeeds(policy, hosts(config.get("seeds")), results);
		List<Object> expected = list(map(row.data.get("expect")).get("snapshots"));

		for (int i = 0; i < expected.size(); i++) {
			String exp = seeds(hosts(expected.get(i)));
			String got = seeds(snapshots.get(i));

			if (! exp.equals(got)) {
				return new Outcome(FAIL, "snapshot " + i + ": expected " + exp + ", got " + got);
			}
		}
		return new Outcome(PASS, null);
	}

	private Outcome tls() throws Exception {
		String tlsName = (String)map(row.data.get("input")).get("tlsName");
		String expected = (String)map(row.data.get("expect")).get("outcome");
		String got;

		try {
			Connection.validateServerCertificate(new TlsPolicy(), tlsName, row.certificate);
			got = "accept";
		}
		catch (AerospikeException ae) {
			if (! ae.getBaseMessage().startsWith(TLS_REJECT_PREFIX)) {
				return new Outcome(FAIL, "threw " + ae);
			}
			got = "reject";
		}

		if (! expected.equals(got)) {
			return new Outcome(FAIL, "expected " + expected + ", got " + got);
		}
		return new Outcome(PASS, null);
	}

	private static SeedMergePolicy mergePolicy(String value) {
		switch (value) {
		case "replace":
			return SeedMergePolicy.REPLACE;

		case "discovery-only":
			return SeedMergePolicy.DISCOVERY_ONLY;

		default:
			return SeedMergePolicy.MERGE;
		}
	}

	private static Host[] hosts(Object value) {
		List<Object> list = list(value);
		Host[] hosts = new Host[list.size()];

		for (int i = 0; i < hosts.length; i++) {
			Map<String,Object> seed = map(list.get(i));
			hosts[i] = new Host((String)seed.get("host"), (Integer)seed.get("port"));
		}
		return hosts;
	}

	private static String seeds(Host[] hosts) {
		return Arrays.stream(hosts).map(h -> h.name + ' ' + h.port).collect(Collectors.joining(", ", "[", "]"));
	}

	private static String endpoint(String host, int port, String tlsName) {
		return "host=" + host + " port=" + port + " tlsName=" + tlsName;
	}

	//-------------------------------------------------------
	// Fixture validation
	//-------------------------------------------------------

	private static List<Row> loadFile(Path path, String file) throws Exception {
		Object doc;

		try (InputStream in = Files.newInputStream(path)) {
			doc = new Yaml(new SafeConstructor(new LoaderOptions())).load(in);
		}

		Map<String,Object> top = requireMap(doc, file);
		checkKeys(top, file, FILE_KEYS, Set.of("kind", "rows"));

		String kind = requireString(top.get("kind"), file + ": kind");

		if (! Set.of("translate", "merge", "tls").contains(kind)) {
			throw malformed(file + ": unknown kind " + kind);
		}

		Map<String,X509Certificate> shapes = null;

		if (kind.equals("tls")) {
			shapes = validateShapes(path, file, top.get("shapes"));
		}
		else if (top.containsKey("shapes")) {
			throw malformed(file + ": shapes is only allowed in a tls file");
		}

		List<Object> list = requireList(top.get("rows"), file + ": rows");

		if (list.isEmpty()) {
			throw malformed(file + ": no rows");
		}

		List<Row> rows = new ArrayList<>();
		Set<String> names = new HashSet<>();

		for (int i = 0; i < list.size(); i++) {
			Map<String,Object> data = requireMap(list.get(i), file + ": row " + i);
			String name = requireString(data.get("case"), file + ": row " + i + " case");
			String where = file + ":" + name;

			if (! names.add(name)) {
				throw malformed(where + ": duplicate case");
			}
			rows.add(validateRow(file, kind, where, data, shapes));
		}
		return rows;
	}

	private static Row validateRow(String file, String kind, String where, Map<String,Object> data,
		Map<String,X509Certificate> shapes) throws Exception {
		checkKeys(data, where, ROW_KEYS, Set.of("case", "because", "input"));
		requireString(data.get("because"), where + ": because");

		String blockedBy = null;

		if (data.containsKey("state")) {
			if (! BLOCKED.equals(data.get("state"))) {
				throw malformed(where + ": state must be blocked");
			}
			blockedBy = requireString(data.get("blockedBy"), where + ": blockedBy");

			if (data.containsKey("expect")) {
				throw malformed(where + ": blocked row has expect");
			}
		}
		else if (data.containsKey("blockedBy")) {
			throw malformed(where + ": blockedBy without state: blocked");
		}
		else if (! data.containsKey("expect")) {
			throw malformed(where + ": missing expect");
		}

		X509Certificate certificate = null;

		switch (kind) {
		case "translate":
			validateTranslate(where, data);
			break;

		case "merge":
			validateMerge(where, data);
			break;

		default:
			certificate = validateTls(where, data, shapes);
			break;
		}
		return new Row(file, kind, (String)data.get("case"), blockedBy, data, certificate);
	}

	private static void validateTranslate(String where, Map<String,Object> data) {
		Map<String,Object> config = requireMap(data.get("config"), where + ": config");
		checkKeys(config, where + ": config", Set.of("legacyHostMap", "translation"), Set.of());

		if (config.containsKey("legacyHostMap")) {
			requireStringMap(config.get("legacyHostMap"), where + ": legacyHostMap");
		}

		if (config.containsKey("translation")) {
			if (config.containsKey("legacyHostMap")) {
				throw malformed(where + ": legacyHostMap and translation are exclusive");
			}

			String at = where + ": translation";
			Map<String,Object> translation = requireMap(config.get("translation"), at);
			checkKeys(translation, at, Set.of("provider", "onMiss", "staticMap", "portMap", "endpoint"), Set.of("provider"));
			String provider = requireOneOf(translation.get("provider"), at + ".provider",
				"static-map", "port-map", "endpoint-port-map");

			if (translation.containsKey("onMiss")) {
				requireOneOf(translation.get("onMiss"), at + ".onMiss", "passThrough", "strict");
			}

			switch (provider) {
			case "static-map":
				checkKeys(translation, at, Set.of("provider", "onMiss", "staticMap"), Set.of("staticMap"));
				requireStringMap(translation.get("staticMap"), at + ".staticMap");
				break;

			case "port-map":
				checkKeys(translation, at, Set.of("provider", "onMiss", "portMap"), Set.of("portMap"));

				for (Object e : requireList(translation.get("portMap"), at + ".portMap")) {
					Map<String,Object> entry = requireMap(e, at + ".portMap");
					checkKeys(entry, at + ".portMap", Set.of("match", "target"), Set.of("match", "target"));
					Map<String,Object> match = requireMap(entry.get("match"), at + ".portMap.match");

					if (match.containsKey("nodeName")) {
						checkKeys(match, at + ".portMap.match", Set.of("nodeName"), Set.of("nodeName"));
						requireString(match.get("nodeName"), at + ".portMap.match.nodeName");
					}
					else {
						requireHost(match, at + ".portMap.match");
					}
					requireHost(entry.get("target"), at + ".portMap.target");
				}
				break;

			default:
				checkKeys(translation, at, Set.of("provider", "onMiss", "endpoint"), Set.of("endpoint"));
				Map<String,Object> endpoint = requireMap(translation.get("endpoint"), at + ".endpoint");
				checkKeys(endpoint, at + ".endpoint", Set.of("hostname", "portBase"), Set.of("hostname", "portBase"));
				requireString(endpoint.get("hostname"), at + ".endpoint.hostname");
				requirePort(endpoint.get("portBase"), at + ".endpoint.portBase");
				break;
			}
		}

		Map<String,Object> input = requireMap(data.get("input"), where + ": input");
		Set<String> inputKeys = Set.of("nodeName", "tlsName", "host", "port", "sourceList");
		checkKeys(input, where + ": input", inputKeys, inputKeys);
		requireString(input.get("nodeName"), where + ": input.nodeName");
		requireTlsName(input.get("tlsName"), where + ": input.tlsName");
		requireString(input.get("host"), where + ": input.host");
		requirePort(input.get("port"), where + ": input.port");
		requireOneOf(input.get("sourceList"), where + ": input.sourceList", "standard", "alternate");

		if (! data.containsKey("expect")) {
			return;
		}

		Map<String,Object> expect = requireMap(data.get("expect"), where + ": expect");

		if (expect.containsKey("error")) {
			checkKeys(expect, where + ": expect", Set.of("error"), Set.of("error"));
			validateMiss(expect.get("error"), where + ": expect.error");
		}
		else {
			checkKeys(expect, where + ": expect", Set.of("host", "port", "tlsName", "diagnostic"),
				Set.of("host", "port", "tlsName"));
			requireString(expect.get("host"), where + ": expect.host");
			requirePort(expect.get("port"), where + ": expect.port");
			requireTlsName(expect.get("tlsName"), where + ": expect.tlsName");

			if (expect.containsKey("diagnostic")) {
				validateMiss(expect.get("diagnostic"), where + ": expect.diagnostic");
			}
		}
	}

	private static void validateMiss(Object value, String where) {
		Map<String,Object> miss = requireMap(value, where);
		checkKeys(miss, where, Set.of("category", "messageContains"), Set.of("category", "messageContains"));
		requireOneOf(miss.get("category"), where + ".category", "translation-miss");
		Map<String,Object> facts = requireMap(miss.get("messageContains"), where + ".messageContains");
		Set<String> keys = Set.of("nodeName", "host", "port", "translator");
		checkKeys(facts, where + ".messageContains", keys, keys);
		requireString(facts.get("nodeName"), where + ".messageContains.nodeName");
		requireString(facts.get("host"), where + ".messageContains.host");
		requirePort(facts.get("port"), where + ".messageContains.port");
		requireOneOf(facts.get("translator"), where + ".messageContains.translator",
			"static-map", "port-map", "endpoint-port-map");
	}

	private static void validateMerge(String where, Map<String,Object> data) {
		Map<String,Object> config = requireMap(data.get("config"), where + ": config");
		checkKeys(config, where + ": config", Set.of("seeds", "discovery"), Set.of("seeds"));
		requireSeeds(config.get("seeds"), where + ": config.seeds");

		if (config.containsKey("discovery")) {
			Map<String,Object> discovery = requireMap(config.get("discovery"), where + ": config.discovery");
			checkKeys(discovery, where + ": config.discovery", Set.of("mergePolicy"), Set.of());

			if (discovery.containsKey("mergePolicy")) {
				requireOneOf(discovery.get("mergePolicy"), where + ": config.discovery.mergePolicy",
					"merge", "replace", "discovery-only");
			}
		}

		Map<String,Object> input = requireMap(data.get("input"), where + ": input");
		checkKeys(input, where + ": input", Set.of("discoveryResults"), Set.of("discoveryResults"));
		List<Object> results = requireList(input.get("discoveryResults"), where + ": input.discoveryResults");

		if (results.isEmpty()) {
			throw malformed(where + ": input.discoveryResults is empty");
		}

		for (Object result : results) {
			requireSeeds(result, where + ": input.discoveryResults");
		}

		if (! data.containsKey("expect")) {
			return;
		}

		Map<String,Object> expect = requireMap(data.get("expect"), where + ": expect");
		checkKeys(expect, where + ": expect", Set.of("snapshots"), Set.of("snapshots"));
		List<Object> snapshots = requireList(expect.get("snapshots"), where + ": expect.snapshots");

		if (snapshots.size() != results.size()) {
			throw malformed(where + ": " + snapshots.size() + " snapshots for " + results.size() + " discovery results");
		}

		for (Object snapshot : snapshots) {
			requireSeeds(snapshot, where + ": expect.snapshots");
		}
	}

	private static X509Certificate validateTls(String where, Map<String,Object> data,
		Map<String,X509Certificate> shapes) {
		Map<String,Object> input = requireMap(data.get("input"), where + ": input");
		checkKeys(input, where + ": input", Set.of("tlsName", "shape"), Set.of("tlsName", "shape"));
		requireString(input.get("tlsName"), where + ": input.tlsName");

		String shape = requireString(input.get("shape"), where + ": input.shape");
		X509Certificate cert = shapes.get(shape);

		if (cert == null) {
			throw malformed(where + ": unknown shape " + shape);
		}

		if (data.containsKey("config")) {
			throw malformed(where + ": unknown key config");
		}

		if (data.containsKey("expect")) {
			Map<String,Object> expect = requireMap(data.get("expect"), where + ": expect");
			checkKeys(expect, where + ": expect", Set.of("outcome"), Set.of("outcome"));
			requireOneOf(expect.get("outcome"), where + ": expect.outcome", "accept", "reject");
		}
		return cert;
	}

	private static Map<String,X509Certificate> validateShapes(Path path, String file, Object value) throws Exception {
		Map<String,Object> map = requireMap(value, file + ": shapes");

		if (map.isEmpty()) {
			throw malformed(file + ": no shapes");
		}

		Map<String,X509Certificate> shapes = new LinkedHashMap<>();

		for (Map.Entry<String,Object> entry : map.entrySet()) {
			String name = entry.getKey();
			String where = file + ": shape " + name;

			if (! SHAPE_NAME.matcher(name).matches()) {
				throw malformed(where + ": invalid name");
			}
			shapes.put(name, validateShape(path.getParent().resolve("certs").resolve(name + ".pem"), where,
				requireMap(entry.getValue(), where)));
		}
		return shapes;
	}

	private static X509Certificate validateShape(Path certFile, String where, Map<String,Object> desc)
		throws Exception {
		Set<String> keys = Set.of("subjectCN", "san");
		checkKeys(desc, where, keys, keys);

		String cn = requireString(desc.get("subjectCN"), where + ": subjectCN");
		List<String> san = new ArrayList<>();

		for (Object name : requireList(desc.get("san"), where + ": san")) {
			san.add(requireString(name, where + ": san"));
		}

		if (! Files.isRegularFile(certFile)) {
			throw malformed(where + ": certificate not found: " + certFile);
		}

		X509Certificate cert;

		try (InputStream in = Files.newInputStream(certFile)) {
			cert = (X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(in);
		}

		List<String> actualCn = new ArrayList<>();
		LdapName subject = new LdapName(cert.getSubjectX500Principal().getName(X500Principal.RFC2253));

		for (Rdn rdn : subject.getRdns()) {
			if (rdn.getType().equalsIgnoreCase("CN")) {
				actualCn.add(rdn.getValue().toString());
			}
		}

		if (! actualCn.equals(List.of(cn))) {
			throw malformed(where + ": certificate CN " + actualCn + " does not match subjectCN " + cn);
		}

		List<String> actualSan = new ArrayList<>();

		if (cert.getSubjectAlternativeNames() != null) {
			for (List<?> name : cert.getSubjectAlternativeNames()) {
				int type = (Integer)name.get(0);
				String value = (String)name.get(1);
				actualSan.add(type == 7? "IP:" + canonicalIp(value) : (type == 2? "DNS:" : type + ":") + value);
			}
		}

		List<String> expectedSan = new ArrayList<>();

		for (String name : san) {
			expectedSan.add(name.startsWith("IP:")? "IP:" + canonicalIp(name.substring(3)) : name);
		}

		if (! actualSan.equals(expectedSan)) {
			throw malformed(where + ": certificate SAN " + actualSan + " does not match san " + san);
		}
		return cert;
	}

	private static String canonicalIp(String literal) throws Exception {
		return InetAddress.getByAddress(InetAddress.getByName(literal).getAddress()).getHostAddress();
	}

	private static void requireSeeds(Object value, String where) {
		for (Object seed : requireList(value, where)) {
			requireHost(seed, where);
		}
	}

	private static void requireHost(Object value, String where) {
		Map<String,Object> host = requireMap(value, where);
		checkKeys(host, where, Set.of("host", "port"), Set.of("host", "port"));
		requireString(host.get("host"), where + ".host");
		requirePort(host.get("port"), where + ".port");
	}

	private static void checkKeys(Map<String,Object> map, String where, Set<String> allowed, Set<String> required) {
		for (String key : map.keySet()) {
			if (! allowed.contains(key)) {
				throw malformed(where + ": unknown key " + key);
			}
		}

		for (String key : required) {
			if (! map.containsKey(key)) {
				throw malformed(where + ": missing " + key);
			}
		}
	}

	private static String requireString(Object value, String where) {
		if (! (value instanceof String) || ((String)value).isEmpty()) {
			throw malformed(where + ": expected a non-empty string, got " + value);
		}
		return (String)value;
	}

	private static void requireTlsName(Object value, String where) {
		if (value != null) {
			requireString(value, where);
		}
	}

	private static void requirePort(Object value, String where) {
		if (! (value instanceof Integer) || (Integer)value < 0 || (Integer)value > MAX_PORT) {
			throw malformed(where + ": expected a port, got " + value);
		}
	}

	private static String requireOneOf(Object value, String where, String... allowed) {
		if (! Arrays.asList(allowed).contains(value)) {
			throw malformed(where + ": expected one of " + Arrays.toString(allowed) + ", got " + value);
		}
		return (String)value;
	}

	private static void requireStringMap(Object value, String where) {
		for (Map.Entry<String,Object> entry : requireMap(value, where).entrySet()) {
			requireString(entry.getValue(), where + "." + entry.getKey());
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String,Object> requireMap(Object value, String where) {
		if (! (value instanceof Map)) {
			throw malformed(where + ": expected a map, got " + value);
		}

		for (Object key : ((Map<?,?>)value).keySet()) {
			if (! (key instanceof String)) {
				throw malformed(where + ": non-string key " + key);
			}
		}
		return (Map<String,Object>)value;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> requireList(Object value, String where) {
		if (! (value instanceof List)) {
			throw malformed(where + ": expected a list, got " + value);
		}
		return (List<Object>)value;
	}

	@SuppressWarnings("unchecked")
	private static Map<String,Object> map(Object value) {
		return (Map<String,Object>)value;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> list(Object value) {
		return (List<Object>)value;
	}

	@SuppressWarnings("unchecked")
	private static Map<String,String> stringMap(Object value) {
		return (Map<String,String>)value;
	}

	private static IllegalStateException malformed(String message) {
		return new IllegalStateException("Malformed conformance fixture: " + message);
	}

	//-------------------------------------------------------
	// Declarations
	//-------------------------------------------------------

	private static void applyDeclarations(Map<String,Map<String,Row>> rows) throws Exception {
		Object doc;

		try (InputStream in = ConformanceTest.class.getResourceAsStream(DECLARED)) {
			if (in == null) {
				throw new IllegalStateException("Declarations not found: " + DECLARED);
			}
			doc = new Yaml(new SafeConstructor(new LoaderOptions())).load(in);
		}

		Map<String,Object> files = (doc == null)? Map.of() : requireDeclaration(doc, DECLARED);

		for (Map.Entry<String,Object> file : files.entrySet()) {
			Map<String,Row> fileRows = rows.get(file.getKey());

			if (fileRows == null) {
				throw new IllegalStateException(DECLARED + ": unknown fixture file " + file.getKey());
			}

			for (Map.Entry<String,Object> c : requireDeclaration(file.getValue(), file.getKey()).entrySet()) {
				String where = DECLARED + ": " + file.getKey() + ":" + c.getKey();
				Row row = fileRows.get(c.getKey());

				if (row == null) {
					throw new IllegalStateException(where + ": unknown case");
				}

				Map<String,Object> decl = requireDeclaration(c.getValue(), where);

				for (String key : decl.keySet()) {
					if (! key.equals("result") && ! key.equals("reason")) {
						throw new IllegalStateException(where + ": unknown key " + key);
					}
				}

				Object result = decl.get("result");
				Object reason = decl.get("reason");

				if (! RESULTS.contains(result)) {
					throw new IllegalStateException(where + ": invalid result " + result);
				}

				if (! PASS.equals(result) && (! (reason instanceof String) || ((String)reason).isEmpty())) {
					throw new IllegalStateException(where + ": a " + result + " declaration needs a reason");
				}
				row.declared = new Declaration((String)result, (String)reason);
			}
		}
	}

	private static Map<String,Object> requireDeclaration(Object value, String where) {
		try {
			return requireMap(value, where);
		}
		catch (IllegalStateException ise) {
			throw new IllegalStateException(DECLARED + ": " + where + " must be a map");
		}
	}

	//-------------------------------------------------------
	// Report
	//-------------------------------------------------------

	private static String clientVersion() throws Exception {
		String version = AerospikeClient.class.getPackage().getImplementationVersion();

		if (version != null) {
			return version;
		}

		Path pom = Paths.get("..", "pom.xml");

		if (Files.isRegularFile(pom)) {
			Matcher m = Pattern.compile("<revision>([^<]+)</revision>").matcher(Files.readString(pom));

			if (m.find()) {
				return m.group(1);
			}
		}
		return "unknown";
	}

	private static void writeJson(StringBuilder sb, Object value, String indent) {
		if (value instanceof Map) {
			Map<?,?> map = (Map<?,?>)value;
			String inner = indent + "  ";
			sb.append('{');
			int i = 0;

			for (Map.Entry<?,?> entry : map.entrySet()) {
				sb.append(i++ == 0? "\n" : ",\n").append(inner);
				writeString(sb, entry.getKey().toString());
				sb.append(": ");
				writeJson(sb, entry.getValue(), inner);
			}
			sb.append('\n').append(indent).append('}');
		}
		else if (value instanceof List) {
			List<?> list = (List<?>)value;
			boolean scalars = list.stream().noneMatch(v -> v instanceof Map || v instanceof List);
			String inner = indent + "  ";
			sb.append('[');

			for (int i = 0; i < list.size(); i++) {
				if (scalars) {
					sb.append(i == 0? "" : ", ");
				}
				else {
					sb.append(i == 0? "\n" : ",\n").append(inner);
				}
				writeJson(sb, list.get(i), inner);
			}

			if (! scalars && ! list.isEmpty()) {
				sb.append('\n').append(indent);
			}
			sb.append(']');
		}
		else if (value instanceof Number || value instanceof Boolean) {
			sb.append(value);
		}
		else if (value == null) {
			sb.append("null");
		}
		else {
			writeString(sb, value.toString());
		}
	}

	private static void writeString(StringBuilder sb, String s) {
		sb.append('"');

		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);

			switch (c) {
			case '"':
				sb.append("\\\"");
				break;

			case '\\':
				sb.append("\\\\");
				break;

			case '\n':
				sb.append("\\n");
				break;

			case '\r':
				sb.append("\\r");
				break;

			case '\t':
				sb.append("\\t");
				break;

			default:
				if (c < 0x20) {
					sb.append(String.format("\\u%04x", (int)c));
				}
				else {
					sb.append(c);
				}
				break;
			}
		}
		sb.append('"');
	}

	//-------------------------------------------------------
	// Types
	//-------------------------------------------------------

	static final class Row {
		final String file;
		final String kind;
		final String name;
		final String blockedBy;
		final Map<String,Object> data;
		final X509Certificate certificate;
		Declaration declared;

		Row(String file, String kind, String name, String blockedBy,
			Map<String,Object> data, X509Certificate certificate) {
			this.file = file;
			this.kind = kind;
			this.name = name;
			this.blockedBy = blockedBy;
			this.data = data;
			this.certificate = certificate;
			this.declared = new Declaration((blockedBy != null)? BLOCKED : PASS, null);
		}

		@Override
		public String toString() {
			return file + ":" + name;
		}
	}

	private static final class Declaration {
		final String result;
		final String reason;

		Declaration(String result, String reason) {
			this.result = result;
			this.reason = reason;
		}
	}

	private static final class Outcome {
		final String result;
		final String reason;

		Outcome(String result, String reason) {
			this.result = result;
			this.reason = reason;
		}
	}
}
