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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
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
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.TestMaps;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConformanceAdapter;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.NodeValidator;
import com.aerospike.client.cluster.Peers;
import com.aerospike.client.command.Buffer;
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
	private static final Set<String> RESULTS = new HashSet<>(Arrays.asList(PASS, FAIL, UNSUPPORTED, BLOCKED));

	private static final String FIXTURE_SET_VERSION = "1.1.0";
	private static final List<String> FIXTURE_ROOTS = Arrays.asList("discovery", "tls");
	private static final String DECLARED = "/conformance/java-declared.yaml";
	private static final Path REPORT = Paths.get("target", "conformance-report.json");
	private static final String TLS_REJECT_PREFIX = "Invalid TLS name: ";
	private static final String FIXTURE_SERVER = "discovery/providers/fixture-server.py";
	private static final int PROVIDER_TIMEOUT = 2000;
	private static final String ACCESS_KEY = "AKIDCONFORMANCE00000";
	private static final String SECRET_KEY = "conformance-secret-key-not-real";
	private static final String SESSION_TOKEN = "conformance-session-token-not-real";
	private static final Map<String,String> ERROR_MARKERS = TestMaps.of(
		"discovery-failed", "DNS seed discovery failed for every name",
		"aws-error", "DescribeInstances failed",
		"timeout", "timed out");
	private static final String COLLISION = "translation-collision";
	private static final Map<Integer,String> RESULT_CATEGORIES = TestMaps.of(ResultCode.TRANSLATION_COLLISION, COLLISION);
	private static final String SEED_NODE = "BB9000000000000";

	private static final List<Map<String,Object>> reportRows = Collections.synchronizedList(new ArrayList<>());
	private static Path dir;
	private static String fixtureSetVersion;
	private static Process fixtureServer;
	private static int fixtureDnsPort;
	private static int fixtureHttpPort;

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

		fixtureSetVersion = new String(Files.readAllBytes(dir.resolve("VERSION")), StandardCharsets.UTF_8).trim();

		if (! FIXTURE_SET_VERSION.equals(fixtureSetVersion)) {
			throw new IllegalStateException("Conformance fixture set " + fixtureSetVersion + " found, " +
				FIXTURE_SET_VERSION + " expected: check out that release or update the declarations");
		}

		List<Path> files = new ArrayList<>();

		for (String root : FIXTURE_ROOTS) {
			try (Stream<Path> stream = Files.walk(dir.resolve(root))) {
				stream.filter(p -> p.toString().endsWith(".yaml")).forEach(files::add);
			}
		}
		Collections.sort(files);

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

		for (String result : Arrays.asList(PASS, FAIL, UNSUPPORTED, BLOCKED)) {
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
		Files.write(REPORT, sb.toString().getBytes(StandardCharsets.UTF_8));
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

		case "collision":
			return collision();

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
		ClientPolicy policy = new ClientPolicy();
		String unsupported = translation(policy, config);

		if (unsupported != null) {
			return new Outcome(UNSUPPORTED, "translator " + unsupported + " is not implemented");
		}

		Endpoint advertised = advertised(input);
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

	private Outcome collision() throws Exception {
		Map<String,Object> config = map(row.data.get("config"));
		Map<String,Object> expect = map(row.data.get("expect"));
		ClientPolicy policy = new ClientPolicy();
		String unsupported = translation(policy, config);

		if (unsupported != null) {
			return new Outcome(UNSUPPORTED, "translator " + unsupported + " is not implemented");
		}

		List<Endpoint> input = new ArrayList<>();

		for (Object item : list(row.data.get("input"))) {
			input.add(advertised(map(item)));
		}

		if (input.stream().map(e -> e.sourceList).distinct().count() > 1) {
			return new Outcome(FAIL, "one peers reply cannot mix standard and alternate peers");
		}

		policy.useServicesAlternate = input.get(0).sourceList == Endpoint.SourceList.ALTERNATE;
		policy.failIfNotConnected = false;

		String reply = input.stream()
			.map(p -> "[" + p.nodeName + "," + p.tlsName + ",[" + (p.host.contains(":")? "[" + p.host + "]" : p.host) +
				":" + p.port + "]]")
			.collect(Collectors.joining(",", "1,3000,[", "]"));
		List<String> logs = new ArrayList<>();

		try (PeersServer server = new PeersServer(reply);
			AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1))) {
			NodeValidator nv = new NodeValidator();

			try {
				nv.validateNode(client.getCluster(), new Host("127.0.0.1", server.port()));
			}
			catch (Throwable e) {
				throw new IllegalStateException("peers server not validated as a node", e);
			}

			PeersNode seed = new PeersNode(client.getCluster(), nv);
			Peers peers = new Peers(input.size());

			for (Endpoint peer : input) {
				try {
					peers.fail(ConformanceAdapter.translate(policy, peer));
				}
				catch (AerospikeException.TranslationMiss tm) {
				}
			}

			Thread thread = Thread.currentThread();
			Log.setLevel(Log.Level.WARN);
			Log.setCallback((level, message) -> {
				if (Thread.currentThread() == thread) {
					logs.add(message);
				}
			});

			try {
				seed.tend(peers);
			}
			finally {
				Log.setCallback(null);
				Log.setLevel(Log.Level.INFO);
				seed.close();
			}

			if (! peers.nodes.isEmpty()) {
				return new Outcome(FAIL, "connected to translated peers " + peers.nodes.keySet());
			}
		}

		List<String> collisions = new ArrayList<>();

		for (String message : logs) {
			if (! COLLISION.equals(category(message))) {
				return new Outcome(FAIL, "unexpected log: " + message);
			}
			collisions.add(message);
		}

		List<Object> errors = list(expect.get("errors"));

		for (Object error : errors) {
			if (collisions.stream().noneMatch(message -> reports(message, map(error)))) {
				return new Outcome(FAIL, "no " + map(error).get("category") + " reported naming " +
					map(error).get("messageContains") + ", got " + collisions);
			}
		}

		for (String message : collisions) {
			if (errors.stream().noneMatch(error -> reports(message, map(error)))) {
				return new Outcome(FAIL, "unexpected collision: " + message);
			}
		}

		Set<String> nodes = new TreeSet<>();

		for (Endpoint peer : input) {
			if (collisions.stream().noneMatch(message -> message.contains(peer.nodeName))) {
				nodes.add(peer.nodeName);
			}
		}

		Set<String> expNodes = new TreeSet<>();

		for (Object name : list(expect.get("nodes"))) {
			expNodes.add((String)name);
		}

		if (! expNodes.equals(nodes)) {
			return new Outcome(FAIL, "expected nodes " + expNodes + ", got " + nodes);
		}
		return new Outcome(PASS, null);
	}

	private static String category(String message) {
		Matcher m = Pattern.compile("^Error (-?\\d+),").matcher(message);
		return m.find()? RESULT_CATEGORIES.get(Integer.parseInt(m.group(1))) : null;
	}

	private static boolean reports(String message, Map<String,Object> error) {
		if (! error.get("category").equals(category(message))) {
			return false;
		}

		for (Object fact : map(error.get("messageContains")).values()) {
			List<?> values = (fact instanceof List)? (List<?>)fact : Arrays.asList(fact);

			for (Object value : values) {
				if (! message.contains(String.valueOf(value))) {
					return false;
				}
			}
		}
		return true;
	}

	private static String translation(ClientPolicy policy, Map<String,Object> config) {
		Map<String,Object> translation = map(config.get("translation"));

		if (translation == null) {
			policy.ipMap = stringMap(config.get("ip_map"));
			return null;
		}

		String provider = (String)translation.get("provider");

		if (translation.containsKey("on_miss")) {
			policy.translationOnMiss = onMiss((String)translation.get("on_miss"));
		}

		switch (provider) {
		case "static-map":
			policy.addressTranslator = new StaticMapAddressTranslator(stringMap(translation.get("static_map")));
			return null;

		case "pass-through":
			policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
			return null;

		case "port-map":
			policy.addressTranslator = new PortMapAddressTranslator(stringMap(translation.get("port_map")));
			return null;

		default:
			return provider;
		}
	}

	private static Endpoint advertised(Map<String,Object> input) {
		Endpoint.SourceList sourceList = input.get("sourceList").equals("alternate")?
			Endpoint.SourceList.ALTERNATE : Endpoint.SourceList.STANDARD;
		return new Endpoint((String)input.get("nodeName"), (String)input.get("tlsName"),
			(String)input.get("host"), (Integer)input.get("port"), sourceList);
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

				for (String secret : Arrays.asList(SECRET_KEY, SESSION_TOKEN)) {
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
			HttpURLConnection conn = (HttpURLConnection)URI.create("http://127.0.0.1:" + fixtureHttpPort +
				"/fixture/select").toURL().openConnection();

			try {
				conn.setRequestMethod("POST");
				conn.setDoOutput(true);

				try (OutputStream out = conn.getOutputStream()) {
					out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
				}

				int status = conn.getResponseCode();
				InputStream body = (status < 400)? conn.getInputStream() : conn.getErrorStream();
				String text = (body != null)? new String(readAll(body), StandardCharsets.UTF_8) : "";

				if (status != 200) {
					throw new IllegalStateException("fixture server rejected the refresh: " + text);
				}
			}
			finally {
				conn.disconnect();
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
			readAll(check.getInputStream());

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
	}

	private static byte[] readAll(InputStream in) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buf = new byte[4096];
		int n;

		try (InputStream is = in) {
			while ((n = is.read(buf)) != -1) {
				out.write(buf, 0, n);
			}
		}
		return out.toByteArray();
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

		Map<String,Object> files = (doc == null)? TestMaps.of() : requireDeclaration(doc, DECLARED);

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
			Matcher m = Pattern.compile("<revision>([^<]+)</revision>").matcher(new String(Files.readAllBytes(pom), StandardCharsets.UTF_8));

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

	private static final class PeersNode extends Node {
		PeersNode(Cluster cluster, NodeValidator nv) {
			super(cluster, nv);
		}

		void tend(Peers peers) {
			refreshPeers(peers);
		}
	}

	/**
	 * Loopback node that answers every peers command with one reply.
	 */
	private static final class PeersServer implements AutoCloseable {
		private final String peers;
		private final ServerSocket server;
		private final List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());

		PeersServer(String peers) throws IOException {
			this.peers = peers;
			this.server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		private void accept() {
			while (true) {
				try {
					Socket socket = server.accept();
					sockets.add(socket);

					Thread thread = new Thread(() -> serve(socket));
					thread.setDaemon(true);
					thread.start();
				}
				catch (IOException ioe) {
					return;
				}
			}
		}

		private void serve(Socket socket) {
			try {
				DataInputStream in = new DataInputStream(socket.getInputStream());
				OutputStream out = socket.getOutputStream();
				byte[] header = new byte[8];

				while (true) {
					in.readFully(header);
					byte[] request = new byte[(int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL)];
					in.readFully(request);

					StringBuilder sb = new StringBuilder();

					for (String command : new String(request, StandardCharsets.UTF_8).split("\n")) {
						if (! command.isEmpty()) {
							sb.append(command).append('\t').append(value(command)).append('\n');
						}
					}

					byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
					byte[] response = new byte[8 + body.length];
					Buffer.longToBytes(body.length | (2L << 56) | (1L << 48), response, 0);
					System.arraycopy(body, 0, response, 8, body.length);
					out.write(response);
					out.flush();
				}
			}
			catch (IOException ioe) {
			}
		}

		private String value(String command) {
			if (command.startsWith("peers-")) {
				return peers;
			}

			switch (command) {
			case "node":
				return SEED_NODE;

			case "partition-generation":
				return "1";

			case "build":
				return "8.1.0.0";

			default:
				return "ok";
			}
		}

		@Override
		public void close() throws IOException {
			server.close();

			synchronized (sockets) {
				for (Socket socket : sockets) {
					socket.close();
				}
			}
		}
	}
}
