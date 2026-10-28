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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

/**
 * Run the shared L1 conformance fixtures against the client and compare each row's result with
 * the result Java declares for it. Fixture format is validated by the conformance repository's CI,
 * so rows are read without checking their shape.
 */
@RunWith(Parameterized.class)
public class ConformanceTest {
	private static final String PASS = "pass";
	private static final String FAIL = "fail";
	private static final String UNSUPPORTED = "unsupported";
	private static final String BLOCKED = "blocked";
	private static final Set<String> RESULTS = Set.of(PASS, FAIL, UNSUPPORTED, BLOCKED);

	private static final String FIXTURE_SET_VERSION = "1.1.0";
	private static final String DECLARED = "/conformance/java-declared.yaml";
	private static final Path REPORT = Paths.get("target", "conformance-report.json");
	private static final String TLS_REJECT_PREFIX = "Invalid TLS name: ";
	private static final String FIXTURE_SERVER = "discovery/providers/fixture-server.py";
	private static final int PROVIDER_TIMEOUT = 2000;
	private static final String ACCESS_KEY = "AKIDCONFORMANCE00000";
	private static final String SECRET_KEY = "conformance-secret-key-not-real";
	private static final String SESSION_TOKEN = "conformance-session-token-not-real";
	private static final Map<String,String> ERROR_MARKERS = Map.of(
		"discovery-failed", "DNS seed discovery failed for every name",
		"aws-error", "DescribeInstances failed",
		"timeout", "timed out");

	private static final List<Map<String,Object>> reportRows = Collections.synchronizedList(new ArrayList<>());
	private static Path dir;
	private static String fixtureSetVersion;
	private static Process fixtureServer;
	private static int fixtureDnsPort;
	private static int fixtureHttpPort;
	private static HttpClient http;

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

		if (! Files.isRegularFile(dir.resolve("VERSION"))) {
			throw new IllegalStateException("Not a conformance fixture set, VERSION not found: " + dir);
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
				.filter(p -> dir.relativize(p).getNameCount() > 1)
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

		for (Object[] param : params) {
			String kind = ((Row)param[1]).kind;

			if (kind.equals("dns") || kind.equals("aws-tags")) {
				startFixtureServer();
				break;
			}
		}
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
	public static void stopFixtureServer() throws Exception {
		if (fixtureServer != null) {
			fixtureServer.destroy();
			fixtureServer.waitFor(5, TimeUnit.SECONDS);
			fixtureServer = null;
		}
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

		case "tls":
			return tls();

		case "dns":
			return provider(dnsProvider());

		case "aws-tags":
			return provider(awsProvider());

		default:
			throw new IllegalStateException(row + ": unknown kind " + row.kind);
		}
	}

	private Outcome translate() {
		Map<String,Object> config = map(row.data.get("config"));
		Map<String,Object> input = map(row.data.get("input"));
		Map<String,Object> expect = map(row.data.get("expect"));
		Map<String,Object> translation = map(config.get("translation"));
		ClientPolicy policy = new ClientPolicy();

		if (translation == null) {
			policy.ipMap = stringMap(config.get("ip_map"));
		}
		else {
			String provider = (String)translation.get("provider");

			if (translation.containsKey("on_miss")) {
				policy.translationOnMiss = onMiss((String)translation.get("on_miss"));
			}

			switch (provider) {
			case "static-map":
				policy.addressTranslator = new StaticMapAddressTranslator(stringMap(translation.get("static_map")));
				break;

			case "pass-through":
				policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
				break;

			case "port-map":
				policy.addressTranslator = new PortMapAddressTranslator(stringMap(translation.get("port_map")));
				break;

			default:
				return new Outcome(UNSUPPORTED, "translator " + provider + " is not implemented");
			}
		}

		Endpoint.SourceList sourceList = input.get("sourceList").equals("alternate")?
			Endpoint.SourceList.ALTERNATE : Endpoint.SourceList.STANDARD;
		Endpoint advertised = new Endpoint((String)input.get("nodeName"), (String)input.get("tlsName"),
			(String)input.get("host"), (Integer)input.get("port"), sourceList);
		Map<String,Object> error = map(expect.get("error"));
		Host actual;

		try {
			actual = ConformanceAdapter.translate(policy, advertised);
		}
		catch (AerospikeException.TranslationMiss tm) {
			if (error == null || ! error.get("category").equals("translation-miss")) {
				return new Outcome(FAIL, "threw " + tm);
			}

			for (Object value : map(error.get("messageContains")).values()) {
				if (! tm.getMessage().contains(String.valueOf(value))) {
					return new Outcome(FAIL, "message does not contain " + value + ": " + tm.getMessage());
				}
			}
			return new Outcome(PASS, null);
		}
		catch (Exception e) {
			return new Outcome(FAIL, "threw " + e);
		}

		if (error != null) {
			return new Outcome(FAIL, "expected " + error.get("category") + " error, got " +
				endpoint(actual.name, actual.port, actual.tlsName));
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

		if (discovery != null && discovery.get("merge_policy") != null) {
			policy.discoveryMergePolicy = mergePolicy((String)discovery.get("merge_policy"));
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

	private DnsSeedCandidateProvider dnsProvider() {
		Map<String,Object> dns = map(map(map(row.data.get("config")).get("discovery")).get("dns"));
		List<String> names = new ArrayList<>();

		for (Object name : list(dns.get("names"))) {
			names.add((String)name);
		}

		DnsSeedCandidateProvider.Builder builder = DnsSeedCandidateProvider.builder()
			.names(names)
			.timeout(intOr(dns.get("timeout_ms"), PROVIDER_TIMEOUT))
			.servers("127.0.0.1:" + fixtureDnsPort);

		if (dns.get("record_type") != null) {
			builder.recordType("srv".equals(dns.get("record_type"))?
				DnsSeedCandidateProvider.RecordType.SRV : DnsSeedCandidateProvider.RecordType.A);
		}

		if (dns.get("port") != null) {
			builder.port((Integer)dns.get("port"));
		}

		if (dns.get("tls_name") != null) {
			builder.tlsName((String)dns.get("tls_name"));
		}
		return builder.build();
	}

	private AwsEc2TagSeedCandidateProvider awsProvider() {
		Map<String,Object> aws = map(map(map(row.data.get("config")).get("discovery")).get("aws"));
		int timeout = intOr(aws.get("timeout_ms"), PROVIDER_TIMEOUT);
		AwsEc2TagSeedCandidateProvider.Builder builder = AwsEc2TagSeedCandidateProvider.builder()
			.region((String)aws.get("region"))
			.timeout(timeout)
			.initTimeout(intOr(aws.get("init_timeout_ms"), Math.max(timeout, PROVIDER_TIMEOUT)))
			.endpoint("http://127.0.0.1:" + fixtureHttpPort)
			.credentials(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);

		for (Object item : list(aws.get("filters"))) {
			Map<String,Object> filter = map(item);
			List<String> values = new ArrayList<>();

			for (Object value : list(filter.get("values"))) {
				values.add((String)value);
			}
			builder.filter((String)filter.get("name"), values);
		}

		if (aws.get("address_field") != null) {
			builder.addressField(AwsEc2TagSeedCandidateProvider.AddressField.valueOf(
				((String)aws.get("address_field")).toUpperCase()));
		}

		if (aws.get("port") != null) {
			builder.port((Integer)aws.get("port"));
		}

		if (aws.get("tls_name") != null) {
			builder.tlsName((String)aws.get("tls_name"));
		}
		return builder.build();
	}

	private Outcome provider(SeedCandidateProvider provider) {
		Map<String,Object> config = map(map(row.data.get("config")).get("discovery"));
		Map<String,Object> aws = map(config.get("aws"));
		String region = (aws != null)? (String)aws.get("region") : null;
		List<Object> refreshes = list(map(row.data.get("input")).get("refreshes"));
		List<Object> expected = list(map(row.data.get("expect")).get("refreshes"));

		List<ConformanceAdapter.Step> steps = ConformanceAdapter.refreshProvider(provider, refreshes.size(),
			i -> selectRefresh(refreshes.get(i), region));

		for (int i = 0; i < expected.size(); i++) {
			Map<String,Object> exp = map(expected.get(i));
			ConformanceAdapter.Step step = steps.get(i);
			Map<String,Object> error = map(exp.get("error"));

			if (error != null) {
				if (step.error == null) {
					return new Outcome(FAIL, "refresh " + i + ": expected " + error.get("category") + ", got " +
						hostSet(step.hosts));
				}

				List<String> facts = new ArrayList<>();
				facts.add(ERROR_MARKERS.get((String)error.get("category")));

				for (Object fact : list(error.get("messageContains"))) {
					facts.add((String)fact);
				}

				for (String fact : facts) {
					if (! step.error.contains(fact)) {
						return new Outcome(FAIL, "refresh " + i + ": message lacks '" + fact + "': " + step.error);
					}
				}

				for (String secret : List.of(SECRET_KEY, SESSION_TOKEN)) {
					if (step.error.contains(secret)) {
						return new Outcome(FAIL, "refresh " + i + ": message contains a credential secret");
					}
				}
			}
			else {
				if (step.error != null) {
					return new Outcome(FAIL, "refresh " + i + ": failed: " + step.error);
				}

				Set<String> expHosts = hostSet(list(exp.get("hosts")));
				Set<String> got = hostSet(step.hosts);

				if (! expHosts.equals(got)) {
					return new Outcome(FAIL, "refresh " + i + ": expected hosts " + expHosts + ", got " + got);
				}
			}

			Set<String> expSeeds = hostSet(list(exp.get("seeds")));
			Set<String> gotSeeds = hostSet(Arrays.asList(step.seeds));

			if (! expSeeds.equals(gotSeeds)) {
				return new Outcome(FAIL, "refresh " + i + ": expected seeds " + expSeeds + ", got " + gotSeeds);
			}
		}
		return new Outcome(PASS, null);
	}

	private static void selectRefresh(Object refresh, String region) {
		Map<String,Object> selection = new LinkedHashMap<>();
		selection.put("refresh", refresh);
		selection.put("region", region);
		StringBuilder sb = new StringBuilder();
		writeJson(sb, selection, "");

		try {
			HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + fixtureHttpPort +
				"/fixture/select")).POST(HttpRequest.BodyPublishers.ofString(sb.toString())).build();
			HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

			if (response.statusCode() != 200) {
				throw new IllegalStateException("fixture server rejected the refresh: " + response.body());
			}
		}
		catch (IllegalStateException e) {
			throw e;
		}
		catch (Exception e) {
			throw new IllegalStateException("fixture server unreachable", e);
		}
	}

	private static Set<String> hostSet(List<?> hosts) {
		Set<String> set = new TreeSet<>();

		if (hosts == null) {
			return set;
		}

		for (Object item : hosts) {
			String host;
			int port;
			String tlsName;

			if (item instanceof Host) {
				Host h = (Host)item;
				host = h.name;
				port = h.port;
				tlsName = h.tlsName;
			}
			else {
				Map<String,Object> h = map(item);
				host = (String)h.get("host");
				port = (Integer)h.get("port");
				tlsName = (String)h.get("tlsName");
			}
			set.add(canonicalHost(host) + " " + port + ((tlsName != null)? " " + tlsName : ""));
		}
		return set;
	}

	private static String canonicalHost(String host) {
		if (host.matches("[0-9.]+") || host.contains(":")) {
			try {
				return InetAddress.getByName(host).getHostAddress();
			}
			catch (Exception e) {
				return host;
			}
		}
		return host;
	}

	private static int intOr(Object value, int otherwise) {
		return (value != null)? (Integer)value : otherwise;
	}

	private static void startFixtureServer() throws Exception {
		Path script = dir.resolve(FIXTURE_SERVER);

		if (! Files.isRegularFile(script)) {
			throw new IllegalStateException("Fixture server not found: " + script);
		}

		try {
			Process check = new ProcessBuilder("python3", "--version").redirectErrorStream(true).start();
			check.getInputStream().readAllBytes();

			if (! check.waitFor(10, TimeUnit.SECONDS) || check.exitValue() != 0) {
				throw new IllegalStateException("python3 --version failed");
			}
		}
		catch (Exception e) {
			throw new IllegalStateException("python3 is required to run the dns and aws-tags fixtures", e);
		}

		Path log = Paths.get("target", "conformance-fixture-server.log");
		Files.createDirectories(log.getParent());
		fixtureServer = new ProcessBuilder("python3", script.toString())
			.redirectError(log.toFile())
			.start();

		BufferedReader reader = new BufferedReader(new InputStreamReader(fixtureServer.getInputStream(),
			StandardCharsets.UTF_8));
		String line = reader.readLine();

		if (line == null) {
			throw new IllegalStateException("Fixture server exited during startup, see " + log);
		}

		Matcher httpPort = Pattern.compile("\"http\": (\\d+)").matcher(line);
		Matcher dnsPort = Pattern.compile("\"dns\": (\\d+)").matcher(line);

		if (! httpPort.find() || ! dnsPort.find()) {
			throw new IllegalStateException("Unexpected fixture server ports line: " + line);
		}
		fixtureHttpPort = Integer.parseInt(httpPort.group(1));
		fixtureDnsPort = Integer.parseInt(dnsPort.group(1));
		http = HttpClient.newHttpClient();
	}

	private static TranslationMissMode onMiss(String value) {
		switch (value) {
		case "pass_through":
			return TranslationMissMode.PASS_THROUGH;

		case "strict":
			return TranslationMissMode.STRICT;

		default:
			throw new IllegalStateException("unknown on_miss " + value);
		}
	}

	private static SeedMergePolicy mergePolicy(String value) {
		switch (value) {
		case "merge":
			return SeedMergePolicy.MERGE;

		case "replace":
			return SeedMergePolicy.REPLACE;

		case "discovery_only":
			return SeedMergePolicy.DISCOVERY_ONLY;

		default:
			throw new IllegalStateException("unknown merge_policy " + value);
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
	// Fixtures
	//-------------------------------------------------------

	private static List<Row> loadFile(Path path, String file) throws Exception {
		Map<String,Object> top;

		try (InputStream in = Files.newInputStream(path)) {
			top = map(new Yaml(new SafeConstructor(new LoaderOptions())).load(in));
		}

		String kind = (String)top.get("kind");
		Map<String,X509Certificate> certificates = new LinkedHashMap<>();
		List<Row> rows = new ArrayList<>();

		for (Object item : list(top.get("rows"))) {
			Map<String,Object> data = map(item);
			X509Certificate certificate = null;

			if (kind.equals("tls")) {
				String shape = (String)map(data.get("input")).get("shape");
				certificate = certificates.computeIfAbsent(shape,
					s -> certificate(path.resolveSibling("certs").resolve(s + ".pem")));
			}
			String blockedBy = BLOCKED.equals(data.get("state"))? (String)data.get("blockedBy") : null;
			rows.add(new Row(file, kind, (String)data.get("case"), blockedBy, data, certificate));
		}
		return rows;
	}

	private static X509Certificate certificate(Path pem) {
		try (InputStream in = Files.newInputStream(pem)) {
			return (X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(in);
		}
		catch (Exception e) {
			throw new IllegalStateException("Cannot read certificate " + pem, e);
		}
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

	@SuppressWarnings("unchecked")
	private static Map<String,Object> requireDeclaration(Object value, String where) {
		if (! (value instanceof Map)) {
			throw new IllegalStateException(DECLARED + ": " + where + " must be a map");
		}
		return (Map<String,Object>)value;
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
