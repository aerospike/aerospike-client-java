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
package com.aerospike.test.discovery;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Stub EC2 Query API endpoint for {@link TestAwsTagsDiscovery}. It answers {@code DescribeInstances}
 * with the configured instances, split into pages of {@link #pageSize} with {@code NextToken}, and
 * records every request. It checks only that each request carries a well-formed SigV4 Authorization
 * header for the expected access key, region and service {@code ec2}, and answers
 * {@code AuthFailure} otherwise. Response bodies follow the EC2 API Reference: the
 * {@code DescribeInstances} response example, and the error response example of "Error codes for the
 * Amazon EC2 API".
 */
public final class StubEc2Server implements AutoCloseable {
	public enum Mode {
		ANSWER,
		UNAUTHORIZED,
		HANG
	}

	public static final String ERROR_REQUEST_ID = "ea966190-f9aa-478e-9ede-example";

	public static final class Request {
		public final Map<String,String> params;
		public final String authorization;
		public final boolean authorizationValid;

		Request(Map<String,String> params, String authorization, boolean authorizationValid) {
			this.params = params;
			this.authorization = authorization;
			this.authorizationValid = authorizationValid;
		}

		public String signature() {
			return (authorization == null)? "" : authorization.substring(authorization.lastIndexOf('=') + 1);
		}
	}

	private final HttpServer server;
	private final ExecutorService executor = Executors.newCachedThreadPool();
	private final Pattern authorization;
	private final List<String[]> instances = new CopyOnWriteArrayList<>();
	private final List<Request> requests = new CopyOnWriteArrayList<>();
	private volatile CountDownLatch release = new CountDownLatch(1);
	private volatile Mode mode = Mode.ANSWER;
	private volatile int pageSize = 1000;

	public StubEc2Server(int port, String accessKey, String region) throws IOException {
		this.authorization = Pattern.compile("AWS4-HMAC-SHA256 Credential=" + Pattern.quote(accessKey) + "/\\d{8}/" +
			Pattern.quote(region) + "/ec2/aws4_request, SignedHeaders=([a-z0-9-]+;)*host(;[a-z0-9-]+)*, " +
			"Signature=[0-9a-f]{64}");
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 64);
		server.setExecutor(executor);
		server.createContext("/", this::handle);
		server.start();
	}

	public void reset() {
		mode(Mode.ANSWER);
		instances.clear();
		requests.clear();
		pageSize = 1000;
	}

	public void instance(String id, String privateIp) {
		instances.add(new String[] {id, privateIp});
	}

	public void pageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public void mode(Mode mode) {
		if (this.mode == Mode.HANG && mode != Mode.HANG) {
			release.countDown();
			release = new CountDownLatch(1);
		}
		this.mode = mode;
	}

	public List<Request> requests() {
		return requests;
	}

	private void handle(HttpExchange exchange) throws IOException {
		Map<String,String> params = form(new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8));
		String auth = exchange.getRequestHeaders().getFirst("Authorization");
		boolean valid = auth != null && authorization.matcher(auth).matches() &&
			exchange.getRequestHeaders().getFirst("X-Amz-Date") != null;
		requests.add(new Request(params, auth, valid));

		if (mode == Mode.HANG) {
			try {
				release.await(60, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			exchange.close();
			return;
		}

		if (! valid) {
			reply(exchange, 401, error("AuthFailure", "AWS was not able to validate the provided access credentials"));
		}
		else if (mode == Mode.UNAUTHORIZED) {
			reply(exchange, 403, error("UnauthorizedOperation", "You are not authorized to perform this operation."));
		}
		else if (! "DescribeInstances".equals(params.get("Action")) || ! "2016-11-15".equals(params.get("Version"))) {
			reply(exchange, 400, error("InvalidAction", "The action or operation requested is not valid."));
		}
		else {
			reply(exchange, 200, describe(params.get("NextToken")));
		}
	}

	private String describe(String nextToken) {
		List<String[]> list = new ArrayList<>(instances);
		int begin = (nextToken == null)? 0 : Integer.parseInt(nextToken.substring("page-".length()));
		int end = Math.min(list.size(), begin + pageSize);
		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		sb.append("<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">\n");
		sb.append("  <requestId>8f7724cf-496f-496e-8fe3-example</requestId>\n  <reservationSet>\n");

		for (int i = begin; i < end; i++) {
			String[] instance = list.get(i);
			sb.append("    <item>\n      <reservationId>r-").append(instance[0]).append("</reservationId>\n");
			sb.append("      <ownerId>123456789012</ownerId>\n      <groupSet/>\n      <instancesSet>\n        <item>\n");
			sb.append("          <instanceId>").append(instance[0]).append("</instanceId>\n");
			sb.append("          <imageId>ami-0abcdef1234567890</imageId>\n");
			sb.append("          <instanceState>\n            <code>16</code>\n            <name>running</name>\n");
			sb.append("          </instanceState>\n");
			sb.append("          <privateDnsName>ip-").append(instance[0]).append(".ec2.internal</privateDnsName>\n");
			sb.append("          <instanceType>c5.large</instanceType>\n");
			sb.append("          <privateIpAddress>").append(instance[1]).append("</privateIpAddress>\n");
			sb.append("          <tagSet>\n            <item>\n              <key>aerospike-cluster</key>\n");
			sb.append("              <value>prod-a</value>\n            </item>\n          </tagSet>\n");
			sb.append("        </item>\n      </instancesSet>\n    </item>\n");
		}
		sb.append("  </reservationSet>\n");

		if (end < list.size()) {
			sb.append("  <nextToken>page-").append(end).append("</nextToken>\n");
		}
		sb.append("</DescribeInstancesResponse>\n");
		return sb.toString();
	}

	private static String error(String code, String message) {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response><Errors><Error><Code>" + code + "</Code><Message>" +
			message + "</Message></Error></Errors><RequestID>" + ERROR_REQUEST_ID + "</RequestID></Response>";
	}

	private static void reply(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add("Content-Type", "text/xml;charset=UTF-8");
		exchange.sendResponseHeaders(status, bytes.length);

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

		for (String pair : body.split("&")) {
			if (pair.isEmpty()) {
				continue;
			}
			int i = pair.indexOf('=');
			map.put(URLDecoder.decode((i < 0)? pair : pair.substring(0, i), "UTF-8"),
				URLDecoder.decode((i < 0)? "" : pair.substring(i + 1), "UTF-8"));
		}
		return map;
	}

	@Override
	public void close() {
		release.countDown();
		server.stop(0);
		executor.shutdownNow();
	}
}
