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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.w3c.dom.Element;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider.Credentials;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider.CredentialsHook;

/**
 * AWS credential chain of {@link AwsEc2TagSeedCandidateProvider}. Sources, first found wins:
 * configured keys, environment, shared credentials file, web identity, ECS container credentials,
 * EC2 IMDSv2. A hook replaces the chain. The chain resolves once; later calls reuse the source that
 * answered. Credentials are cached and reloaded on the calling thread when they are within
 * {@link #REFRESH_MARGIN} of expiry. Used by one provider instance on one thread at a time.
 */
final class AwsCredentialChain {
	static final long REFRESH_MARGIN = TimeUnit.MINUTES.toMillis(5);

	private static final String FORM = "application/x-www-form-urlencoded; charset=utf-8";
	private static final String IMDS_TOKEN_TTL = "21600";
	private static final String IMDS_CREDENTIALS = "/latest/meta-data/iam/security-credentials/";

	enum Source {
		CONFIG("config"),
		ENVIRONMENT("environment"),
		PROFILE("profile"),
		WEB_IDENTITY("web-identity"),
		ECS("ecs"),
		IMDS("imds"),
		HOOK("hook");

		final String label;

		Source(String label) {
			this.label = label;
		}
	}

	private final Credentials config;
	private final CredentialsHook hook;
	private final AwsEnvironment env;
	private final String region;
	private final AwsXml xml;
	private Source source;
	private Credentials cached;
	private String imdsRole;

	AwsCredentialChain(Credentials config, CredentialsHook hook, AwsEnvironment env, String region, AwsXml xml) {
		this.config = config;
		this.hook = hook;
		this.env = env;
		this.region = region;
		this.xml = xml;
	}

	/**
	 * Return credentials, loading them when none are cached or the cached ones are within
	 * {@link #REFRESH_MARGIN} of expiry. {@code deadline} is a {@link System#nanoTime()} value.
	 *
	 * @throws AerospikeException	if no credentials are found, a source fails or the deadline passes
	 */
	Credentials get(long deadline) {
		long now = env.currentTimeMillis();

		if (cached != null && (cached.getExpiration() == 0 || now < cached.getExpiration() - REFRESH_MARGIN)) {
			return cached;
		}

		try {
			Credentials credentials = (source == null)? resolve(deadline) : load(source, deadline);
			cached = credentials;
			return credentials;
		}
		catch (AerospikeException ae) {
			if (cached != null && now < cached.getExpiration()) {
				if (Log.warnEnabled()) {
					Log.warn("AWS EC2 seed provider: credentials refresh from " + source.label +
						" failed, using cached credentials until they expire: " + ae.getBaseMessage());
				}
				return cached;
			}
			throw ae;
		}
	}

	/**
	 * Return the source that answered, or null before the chain has resolved.
	 */
	Source source() {
		return source;
	}

	private Credentials resolve(long deadline) {
		Source found;

		if (hook != null) {
			found = Source.HOOK;
		}
		else if (config != null) {
			found = Source.CONFIG;
		}
		else if (environment() != null) {
			found = Source.ENVIRONMENT;
		}
		else if (profile() != null) {
			found = Source.PROFILE;
		}
		else if (present("AWS_WEB_IDENTITY_TOKEN_FILE") && present("AWS_ROLE_ARN")) {
			found = Source.WEB_IDENTITY;
		}
		else if (present("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") || present("AWS_CONTAINER_CREDENTIALS_FULL_URI")) {
			found = Source.ECS;
		}
		else if (! "true".equalsIgnoreCase(env.getenv("AWS_EC2_METADATA_DISABLED"))) {
			found = Source.IMDS;
		}
		else {
			throw new AerospikeException("AWS EC2 seed provider: no AWS credentials found in config, environment, " +
				"shared credentials file, web identity or ECS, and IMDS is disabled");
		}

		Credentials credentials;

		try {
			credentials = load(found, deadline);
		}
		catch (AerospikeException ae) {
			if (found == Source.IMDS) {
				throw new AerospikeException("AWS EC2 seed provider: no AWS credentials found in config, " +
					"environment, shared credentials file, web identity or ECS, and " +
					ae.getBaseMessage().replace("AWS EC2 seed provider: ", ""), ae);
			}
			throw ae;
		}

		source = found;

		if (Log.infoEnabled()) {
			Log.info("AWS EC2 seed provider credentials from " + found.label);
		}
		return credentials;
	}

	private Credentials load(Source from, long deadline) {
		switch (from) {
		case HOOK:
			return hook();

		case CONFIG:
			return config;

		case ENVIRONMENT:
			return environment();

		case PROFILE:
			return profile();

		case WEB_IDENTITY:
			return webIdentity(deadline);

		case ECS:
			return ecs(deadline);

		default:
			return imds(deadline);
		}
	}

	private Credentials hook() {
		Credentials credentials;

		try {
			credentials = hook.get();
		}
		catch (Throwable e) {
			throw new AerospikeException("AWS EC2 seed provider: credentials hook failed: " + e.getClass().getName(), e);
		}

		if (credentials == null) {
			throw new AerospikeException("AWS EC2 seed provider: credentials hook returned null");
		}
		return credentials;
	}

	private Credentials environment() {
		String key = env.getenv("AWS_ACCESS_KEY_ID");
		String secret = env.getenv("AWS_SECRET_ACCESS_KEY");

		if (key == null || key.isEmpty() || secret == null || secret.isEmpty()) {
			return null;
		}
		return new Credentials(key, secret, empty(env.getenv("AWS_SESSION_TOKEN")));
	}

	private Credentials profile() {
		String file = env.getenv("AWS_SHARED_CREDENTIALS_FILE");
		Path path;

		if (file == null || file.isEmpty()) {
			path = Paths.get(env.userHome(), ".aws", "credentials");
		}
		else if (file.startsWith("~/")) {
			path = Paths.get(env.userHome(), file.substring(2));
		}
		else {
			path = Paths.get(file);
		}

		if (! Files.isReadable(path)) {
			return null;
		}

		String profile = env.getenv("AWS_PROFILE");

		if (profile == null || profile.isEmpty()) {
			profile = "default";
		}

		List<String> lines;

		try {
			lines = Files.readAllLines(path, StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			throw new AerospikeException("AWS EC2 seed provider: failed to read " + path + ": " + e, e);
		}

		Map<String,String> keys = new HashMap<>();
		boolean inProfile = false;

		for (String line : lines) {
			line = line.trim();

			if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
				continue;
			}

			if (line.startsWith("[") && line.endsWith("]")) {
				inProfile = line.substring(1, line.length() - 1).trim().equals(profile);
				continue;
			}

			int i = line.indexOf('=');

			if (inProfile && i > 0) {
				keys.put(line.substring(0, i).trim(), line.substring(i + 1).trim());
			}
		}

		String key = keys.get("aws_access_key_id");
		String secret = keys.get("aws_secret_access_key");

		if (key == null || key.isEmpty() || secret == null || secret.isEmpty()) {
			return null;
		}
		return new Credentials(key, secret, empty(keys.get("aws_session_token")));
	}

	private Credentials webIdentity(long deadline) {
		String token;

		try {
			token = new String(Files.readAllBytes(Paths.get(env.getenv("AWS_WEB_IDENTITY_TOKEN_FILE"))),
				StandardCharsets.UTF_8).trim();
		}
		catch (IOException e) {
			throw new AerospikeException("AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: " + e, e);
		}

		String session = env.getenv("AWS_ROLE_SESSION_NAME");

		if (session == null || session.isEmpty()) {
			session = "aerospike-client-" + env.currentTimeMillis();
		}

		String body = "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=" +
			AwsSigV4.encode(env.getenv("AWS_ROLE_ARN")) + "&RoleSessionName=" + AwsSigV4.encode(session) +
			"&WebIdentityToken=" + AwsSigV4.encode(token);
		Map<String,String> headers = new TreeMap<>();
		headers.put("content-type", FORM);

		AwsHttp.Response response = send("STS AssumeRoleWithWebIdentity", "POST", env.stsEndpoint(region), headers,
			body.getBytes(StandardCharsets.UTF_8), deadline, token);

		if (response.status != 200) {
			throw xml.error("STS AssumeRoleWithWebIdentity", response, token);
		}

		Element credentials;

		try {
			credentials = (Element)xml.parse(response.body).getElementsByTagName("Credentials").item(0);
		}
		catch (Exception e) {
			throw new AerospikeException("AWS EC2 seed provider: invalid STS AssumeRoleWithWebIdentity response");
		}

		if (credentials == null) {
			throw new AerospikeException("AWS EC2 seed provider: STS AssumeRoleWithWebIdentity response has no credentials");
		}
		return temporary("STS AssumeRoleWithWebIdentity", AwsXml.text(credentials, "AccessKeyId"),
			AwsXml.text(credentials, "SecretAccessKey"), AwsXml.text(credentials, "SessionToken"),
			AwsXml.text(credentials, "Expiration"));
	}

	private Credentials ecs(long deadline) {
		String relative = env.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
		String uri = (relative != null && ! relative.isEmpty())? env.ecsEndpoint() + relative :
			env.getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI");
		String authorization = null;
		String file = env.getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");

		if (file != null && ! file.isEmpty()) {
			try {
				authorization = new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8).trim();
			}
			catch (IOException e) {
				throw new AerospikeException("AWS EC2 seed provider: failed to read AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: " +
					e, e);
			}
		}
		else {
			authorization = empty(env.getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN"));
		}

		Map<String,String> headers = new TreeMap<>();

		if (authorization != null) {
			headers.put("authorization", authorization);
		}

		AwsHttp.Response response = send("ECS credentials", "GET", uri, headers, null, deadline, authorization);

		if (response.status != 200) {
			throw new AerospikeException("AWS EC2 seed provider: ECS credentials failed: HTTP " + response.status);
		}

		Map<String,String> json = json(response, "ECS credentials");
		return temporary("ECS credentials", json.get("AccessKeyId"), json.get("SecretAccessKey"), json.get("Token"),
			json.get("Expiration"));
	}

	private Credentials imds(long deadline) {
		String base = env.imdsEndpoint();
		Map<String,String> headers = new TreeMap<>();
		headers.put("x-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL);

		AwsHttp.Response response = send("IMDS token", "PUT", base + "/latest/api/token", headers, new byte[0], deadline);

		if (response.status != 200) {
			throw new AerospikeException("AWS EC2 seed provider: IMDS token failed: HTTP " + response.status);
		}

		String token = response.text().trim();
		headers.clear();
		headers.put("x-aws-ec2-metadata-token", token);

		if (imdsRole == null) {
			response = send("IMDS role", "GET", base + IMDS_CREDENTIALS, headers, null, deadline, token);

			if (response.status != 200) {
				throw new AerospikeException("AWS EC2 seed provider: IMDS role failed: HTTP " + response.status +
					" (no instance profile attached?)");
			}

			String role = response.text().trim();
			int i = role.indexOf('\n');
			role = (i >= 0)? role.substring(0, i).trim() : role;

			if (role.isEmpty()) {
				throw new AerospikeException("AWS EC2 seed provider: IMDS returned no instance profile role");
			}
			imdsRole = role;
		}

		response = send("IMDS credentials", "GET", base + IMDS_CREDENTIALS + imdsRole, headers, null, deadline, token);

		if (response.status != 200) {
			throw new AerospikeException("AWS EC2 seed provider: IMDS credentials failed: HTTP " + response.status);
		}

		Map<String,String> json = json(response, "IMDS credentials");
		String code = json.get("Code");

		if (code != null && ! code.equals("Success")) {
			throw new AerospikeException("AWS EC2 seed provider: IMDS credentials failed: " + code);
		}
		return temporary("IMDS credentials", json.get("AccessKeyId"), json.get("SecretAccessKey"), json.get("Token"),
			json.get("Expiration"));
	}

	private AwsHttp.Response send(String action, String method, String uri, Map<String,String> headers, byte[] body,
		long deadline, String... secrets) {
		try {
			return AwsHttp.request(method, URI.create(uri), headers, body, deadline);
		}
		catch (SocketTimeoutException e) {
			throw new AerospikeException("AWS EC2 seed provider: " + action + " timed out");
		}
		catch (IOException | IllegalArgumentException e) {
			throw new AerospikeException("AWS EC2 seed provider: " + action + " failed: " +
				AwsXml.redact(e.toString(), secrets));
		}
	}

	private static Credentials temporary(String action, String key, String secret, String token, String expiration) {
		if (key == null || key.isEmpty() || secret == null || secret.isEmpty() || expiration == null) {
			throw new AerospikeException("AWS EC2 seed provider: " + action + " response has no credentials");
		}

		long expires;

		try {
			expires = Instant.parse(expiration).toEpochMilli();
		}
		catch (RuntimeException e) {
			throw new AerospikeException("AWS EC2 seed provider: " + action + " returned an invalid expiration: " +
				expiration);
		}
		return new Credentials(key, secret, empty(token), expires);
	}

	private static Map<String,String> json(AwsHttp.Response response, String action) {
		try {
			return Json.object(response.text());
		}
		catch (RuntimeException e) {
			throw new AerospikeException("AWS EC2 seed provider: invalid " + action + " response");
		}
	}

	private boolean present(String name) {
		String value = env.getenv(name);
		return value != null && ! value.isEmpty();
	}

	private static String empty(String value) {
		return (value == null || value.isEmpty())? null : value;
	}

	/**
	 * Parser for the flat JSON objects returned by the ECS and IMDS credential endpoints. Values are
	 * returned as strings; nested objects and arrays are skipped.
	 */
	static final class Json {
		private final String s;
		private int i;

		private Json(String s) {
			this.s = s;
		}

		static Map<String,String> object(String text) {
			Json json = new Json(text);
			Map<String,String> map = new LinkedHashMap<>();
			json.expect('{');

			if (json.peek() == '}') {
				json.i++;
				return map;
			}

			while (true) {
				String key = json.string();
				json.expect(':');
				String value = json.value();

				if (value != null) {
					map.put(key, value);
				}

				char c = json.next();

				if (c == '}') {
					return map;
				}
				if (c != ',') {
					throw new IllegalArgumentException("expected , or }");
				}
			}
		}

		private char peek() {
			while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
				i++;
			}
			if (i >= s.length()) {
				throw new IllegalArgumentException("unexpected end");
			}
			return s.charAt(i);
		}

		private char next() {
			char c = peek();
			i++;
			return c;
		}

		private void expect(char c) {
			if (next() != c) {
				throw new IllegalArgumentException("expected " + c);
			}
		}

		private String value() {
			char c = peek();

			if (c == '"') {
				return string();
			}

			if (c == '{' || c == '[') {
				skip();
				return null;
			}

			int begin = i;

			while (i < s.length() && ",}] \t\r\n".indexOf(s.charAt(i)) < 0) {
				i++;
			}

			String literal = s.substring(begin, i);
			return literal.equals("null")? null : literal;
		}

		private void skip() {
			int depth = 0;

			do {
				char c = peek();

				if (c == '"') {
					string();
					continue;
				}
				if (c == '{' || c == '[') {
					depth++;
				}
				else if (c == '}' || c == ']') {
					depth--;
				}
				i++;
			} while (depth > 0);
		}

		private String string() {
			expect('"');
			StringBuilder sb = new StringBuilder();

			while (true) {
				if (i >= s.length()) {
					throw new IllegalArgumentException("unterminated string");
				}

				char c = s.charAt(i++);

				if (c == '"') {
					return sb.toString();
				}

				if (c != '\\') {
					sb.append(c);
					continue;
				}

				c = s.charAt(i++);

				switch (c) {
				case 'b': sb.append('\b'); break;
				case 'f': sb.append('\f'); break;
				case 'n': sb.append('\n'); break;
				case 'r': sb.append('\r'); break;
				case 't': sb.append('\t'); break;
				case 'u':
					sb.append((char)Integer.parseInt(s.substring(i, i + 4), 16));
					i += 4;
					break;
				default: sb.append(c); break;
				}
			}
		}
	}
}
