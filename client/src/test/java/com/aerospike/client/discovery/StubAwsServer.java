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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * In-process AWS endpoints on one loopback port, for tests: EC2 Query API at {@code /}, STS at
 * {@code /sts}, ECS container credentials at {@code /ecs/...} and IMDSv2 at {@code /latest/...}.
 * Every request is recorded. Handlers run on the server's dispatcher thread, started by the
 * constructor. Response bodies follow the EC2, STS and IMDS documentation examples, see the tests.
 */
public final class StubAwsServer implements AutoCloseable {
	public static final String IMDS_TOKEN = "imds-session-token";
	public static final String IMDS_ROLE = "aerospike-seed-role";
	public static final String ECS_AUTHORIZATION = "ecs-authorization-token";

	public static final class Request {
		public final String method;
		public final String path;
		public final Map<String,String> headers;
		public final Map<String,String> params;

		Request(String method, String path, Map<String,String> headers, Map<String,String> params) {
			this.method = method;
			this.path = path;
			this.headers = headers;
			this.params = params;
		}

		public String accessKey() {
			String auth = headers.get("authorization");

			if (auth == null) {
				return null;
			}
			int begin = auth.indexOf("Credential=") + 11;
			return auth.substring(begin, auth.indexOf('/', begin));
		}

		@Override
		public String toString() {
			return method + ' ' + path + ' ' + params;
		}
	}

	public static final class Reply {
		final int status;
		final String body;

		public Reply(int status, String body) {
			this.status = status;
			this.body = body;
		}
	}

	private final HttpServer server;
	private final List<Request> requests = new CopyOnWriteArrayList<>();
	private final Deque<Reply> ec2 = new ArrayDeque<>();
	private final Map<String,AtomicInteger> counts = new ConcurrentHashMap<>();
	private volatile Reply ec2Default = new Reply(200, describe(null));
	private volatile Reply sts;
	private volatile String credentialsJson;

	public StubAwsServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
		server.createContext("/", this::handle);
		server.start();
	}

	public String url() {
		return "http://127.0.0.1:" + server.getAddress().getPort();
	}

	public synchronized void ec2(Reply... replies) {
		Collections.addAll(ec2, replies);
	}

	public void ec2Default(Reply reply) {
		ec2Default = reply;
	}

	public void sts(Reply reply) {
		sts = reply;
	}

	public void credentials(String json) {
		credentialsJson = json;
	}

	public List<Request> requests() {
		return requests;
	}

	public int count(String path) {
		AtomicInteger n = counts.get(path);
		return (n == null)? 0 : n.get();
	}

	private synchronized Reply nextEc2() {
		Reply reply = ec2.poll();
		return (reply != null)? reply : ec2Default;
	}

	private void handle(HttpExchange exchange) throws IOException {
		String method = exchange.getRequestMethod();
		String path = exchange.getRequestURI().getPath();
		Map<String,String> headers = new LinkedHashMap<>();

		for (Map.Entry<String,List<String>> entry : exchange.getRequestHeaders().entrySet()) {
			headers.put(entry.getKey().toLowerCase(), entry.getValue().get(0));
		}

		String body = new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8);
		Request request = new Request(method, path, headers, form(body));
		requests.add(request);
		counts.computeIfAbsent(path, k -> new AtomicInteger()).incrementAndGet();

		Reply reply;

		if (path.equals("/")) {
			reply = nextEc2();
		}
		else if (path.equals("/sts")) {
			reply = sts;
		}
		else if (path.startsWith("/ecs/")) {
			reply = ECS_AUTHORIZATION.equals(headers.get("authorization"))? new Reply(200, credentialsJson) :
				new Reply(401, "{\"message\":\"unauthorized\"}");
		}
		else if (path.equals("/latest/api/token")) {
			reply = (method.equals("PUT") && headers.containsKey("x-aws-ec2-metadata-token-ttl-seconds"))?
				new Reply(200, IMDS_TOKEN) : new Reply(400, "");
		}
		else if (path.startsWith("/latest/meta-data/iam/security-credentials/")) {
			if (! IMDS_TOKEN.equals(headers.get("x-aws-ec2-metadata-token"))) {
				reply = new Reply(401, "");
			}
			else if (path.endsWith("/")) {
				reply = new Reply(200, IMDS_ROLE + "\n");
			}
			else {
				reply = path.endsWith("/" + IMDS_ROLE)? new Reply(200, credentialsJson) : new Reply(404, "");
			}
		}
		else {
			reply = new Reply(404, "");
		}

		if (reply == null) {
			reply = new Reply(500, "");
		}

		byte[] bytes = reply.body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add("Content-Type", path.equals("/") || path.equals("/sts")? "text/xml" : "text/plain");
		exchange.sendResponseHeaders(reply.status, bytes.length == 0? -1 : bytes.length);

		try (OutputStream out = exchange.getResponseBody()) {
			out.write(bytes);
		}
	}

	private static byte[] readAll(InputStream in) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buf = new byte[4096];
		int n;

		while ((n = in.read(buf)) > 0) {
			out.write(buf, 0, n);
		}
		return out.toByteArray();
	}

	private static Map<String,String> form(String body) throws IOException {
		Map<String,String> map = new LinkedHashMap<>();

		if (body.isEmpty()) {
			return map;
		}

		for (String pair : body.split("&")) {
			int i = pair.indexOf('=');
			String key = (i < 0)? pair : pair.substring(0, i);
			String value = (i < 0)? "" : pair.substring(i + 1);
			map.put(URLDecoder.decode(key, "UTF-8"), URLDecoder.decode(value, "UTF-8"));
		}
		return map;
	}

	/**
	 * Return a DescribeInstances response with the given instance XML fragments in one reservation,
	 * modelled on the response example of the EC2 API Reference, DescribeInstances, Example 1.
	 */
	public static String describe(String nextToken, String... instances) {
		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		sb.append("<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">\n");
		sb.append("  <requestId>8f7724cf-496f-496e-8fe3-example</requestId>\n");
		sb.append("  <reservationSet>\n");

		if (instances.length > 0) {
			sb.append("    <item>\n      <reservationId>r-1234567890abcdef0</reservationId>\n");
			sb.append("      <ownerId>123456789012</ownerId>\n      <groupSet/>\n      <instancesSet>\n");

			for (String instance : instances) {
				sb.append(instance);
			}
			sb.append("      </instancesSet>\n    </item>\n");
		}
		sb.append("  </reservationSet>\n");

		if (nextToken != null) {
			sb.append("  <nextToken>").append(nextToken).append("</nextToken>\n");
		}
		sb.append("</DescribeInstancesResponse>\n");
		return sb.toString();
	}

	/**
	 * Return one {@code instancesSet} item. A null field is omitted. The network interface carries a
	 * different private address, which must not be returned.
	 */
	public static String instance(String id, String privateIp, String publicIp, String privateDns, String publicDns) {
		StringBuilder sb = new StringBuilder();
		sb.append("        <item>\n          <instanceId>").append(id).append("</instanceId>\n");
		sb.append("          <imageId>ami-0abcdef1234567890</imageId>\n");
		sb.append("          <instanceState>\n            <code>16</code>\n            <name>running</name>\n");
		sb.append("          </instanceState>\n");
		field(sb, "privateDnsName", privateDns);
		field(sb, "dnsName", publicDns);
		sb.append("          <instanceType>c5.large</instanceType>\n");
		field(sb, "privateIpAddress", privateIp);
		field(sb, "ipAddress", publicIp);
		sb.append("          <networkInterfaceSet>\n            <item>\n");
		sb.append("              <privateIpAddress>192.0.2.254</privateIpAddress>\n");
		sb.append("            </item>\n          </networkInterfaceSet>\n");
		sb.append("          <tagSet>\n            <item>\n              <key>aerospike-cluster</key>\n");
		sb.append("              <value>prod-a</value>\n            </item>\n          </tagSet>\n");
		sb.append("        </item>\n");
		return sb.toString();
	}

	private static void field(StringBuilder sb, String name, String value) {
		if (value != null) {
			sb.append("          <").append(name).append('>').append(value).append("</").append(name).append(">\n");
		}
	}

	/**
	 * Return an EC2 error response, in the format of the EC2 API Reference, Error codes, error
	 * response example.
	 */
	public static String error(String code, String message, String requestId) {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response><Errors><Error><Code>" + code +
			"</Code><Message>" + message + "</Message></Error></Errors><RequestID>" + requestId +
			"</RequestID></Response>";
	}

	/**
	 * Return an STS AssumeRoleWithWebIdentity response, modelled on the example of the STS API
	 * Reference, AssumeRoleWithWebIdentity.
	 */
	public static String assumeRoleWithWebIdentity(String key, String secret, String token, String expiration) {
		return "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n" +
			"  <AssumeRoleWithWebIdentityResult>\n" +
			"    <SubjectFromWebIdentityToken>system:serviceaccount:aerospike:client</SubjectFromWebIdentityToken>\n" +
			"    <Audience>sts.amazonaws.com</Audience>\n" +
			"    <AssumedRoleUser>\n" +
			"      <Arn>arn:aws:sts::123456789012:assumed-role/seed-role/app1</Arn>\n" +
			"      <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:app1</AssumedRoleId>\n" +
			"    </AssumedRoleUser>\n" +
			"    <Credentials>\n" +
			"      <SessionToken>" + token + "</SessionToken>\n" +
			"      <SecretAccessKey>" + secret + "</SecretAccessKey>\n" +
			"      <Expiration>" + expiration + "</Expiration>\n" +
			"      <AccessKeyId>" + key + "</AccessKeyId>\n" +
			"    </Credentials>\n" +
			"    <Provider>oidc.eks.us-east-1.amazonaws.com</Provider>\n" +
			"  </AssumeRoleWithWebIdentityResult>\n" +
			"  <ResponseMetadata>\n" +
			"    <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>\n" +
			"  </ResponseMetadata>\n" +
			"</AssumeRoleWithWebIdentityResponse>\n";
	}

	/**
	 * Return a credentials document as returned by IMDS (EC2 User Guide, "Retrieve security
	 * credentials from instance metadata") and, with the same keys, by the ECS credentials endpoint.
	 */
	public static String credentialsJson(String key, String secret, String token, String expiration) {
		return "{\n  \"Code\" : \"Success\",\n  \"LastUpdated\" : \"2012-04-26T16:39:16Z\",\n" +
			"  \"Type\" : \"AWS-HMAC\",\n  \"AccessKeyId\" : \"" + key + "\",\n  \"SecretAccessKey\" : \"" + secret +
			"\",\n  \"Token\" : \"" + token + "\",\n  \"Expiration\" : \"" + expiration + "\"\n}";
	}

	/**
	 * Return a server socket that accepts connections into its backlog and never answers.
	 */
	public static ServerSocket silent() throws IOException {
		return new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
	}

	@Override
	public void close() {
		server.stop(0);
	}
}
