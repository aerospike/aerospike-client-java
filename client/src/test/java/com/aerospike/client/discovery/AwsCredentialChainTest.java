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

import static com.aerospike.client.discovery.AwsEc2TagSeedCandidateProviderTest.KEY;
import static com.aerospike.client.discovery.AwsEc2TagSeedCandidateProviderTest.SECRET;
import static com.aerospike.client.discovery.AwsEc2TagSeedCandidateProviderTest.TOKEN;
import static com.aerospike.client.discovery.AwsEc2TagSeedCandidateProviderTest.assertNoSecret;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider.Credentials;
import com.aerospike.client.discovery.StubAwsServer.Reply;
import com.aerospike.client.discovery.StubAwsServer.Request;

/**
 * Each credential source of the AWS credential chain against {@link StubAwsServer}, chain order,
 * expiry and the hook.
 */
public class AwsCredentialChainTest {
	private static final SeedCandidateContext INIT = new SeedCandidateContext(null, new Host[0], 0);
	private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/seed-role";
	private static final String WEB_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVYQU1QTEUifQ.web-identity-token";

	private final List<String> messages = new CopyOnWriteArrayList<>();
	private StubAwsServer stub;
	private TestAwsEnvironment env;
	private Path dir;

	@Before
	public void setUp() throws Exception {
		stub = new StubAwsServer();
		env = new TestAwsEnvironment(stub.url());
		dir = Files.createTempDirectory("aws-chain");
		env.home = dir.toString();
		Log.setLevel(Log.Level.INFO);
		Log.setCallback((level, message) -> messages.add(level + " " + message));
	}

	@After
	public void tearDown() throws Exception {
		stub.close();
		Log.setCallback(null);

		try (java.util.stream.Stream<Path> files = Files.walk(dir)) {
			files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
		}
	}

	private AwsEc2TagSeedCandidateProvider.Builder builder() {
		return AwsEc2TagSeedCandidateProvider.builder().region("us-east-1").tagFilter("aerospike-cluster", "prod-a")
			.environment(env);
	}

	private Request lastEc2() {
		Request last = null;

		for (Request request : stub.requests()) {
			if (request.path.equals("/")) {
				last = request;
			}
		}
		return last;
	}

	private String refresh(AwsEc2TagSeedCandidateProvider provider) {
		provider.refreshSeedCandidates(INIT);
		Request request = lastEc2();
		String line = "AWS-TAGS-CREDENTIALS source=" + provider.credentialSource() + " accessKey=" + request.accessKey() +
			" sessionTokenHeader=" + (request.headers.get("x-amz-security-token") != null);
		System.out.println(line);
		return request.accessKey();
	}

	private void env(String key) {
		env.set("AWS_ACCESS_KEY_ID", key).set("AWS_SECRET_ACCESS_KEY", SECRET).set("AWS_SESSION_TOKEN", TOKEN);
	}

	private Path profileFile(String name) throws Exception {
		Path file = dir.resolve(name);
		Files.write(file, ("# shared credentials\n[default]\naws_access_key_id = AKIADEFAULT\n" +
			"aws_secret_access_key = " + SECRET + "\n\n[seed]\naws_access_key_id=AKIASEEDPROFILE\n" +
			"aws_secret_access_key=" + SECRET + "\naws_session_token=" + TOKEN + "\n\n[sso-only]\n" +
			"sso_session = corp\n").getBytes(StandardCharsets.UTF_8));
		return file;
	}

	private void webIdentity() throws Exception {
		Path token = dir.resolve("web-identity-token");
		Files.write(token, (WEB_TOKEN + "\n").getBytes(StandardCharsets.UTF_8));
		env.set("AWS_WEB_IDENTITY_TOKEN_FILE", token.toString()).set("AWS_ROLE_ARN", ROLE_ARN)
			.set("AWS_ROLE_SESSION_NAME", "seed-session");
		stub.sts(new Reply(200, StubAwsServer.assumeRoleWithWebIdentity("ASIASTS", SECRET, TOKEN, "2099-01-01T00:00:00Z")));
	}

	@Test
	public void configKeys() {
		env("AKIAENV");
		assertEquals(KEY, refresh(builder().credentials(KEY, SECRET, null).build()));
		assertNull(lastEc2().headers.get("x-amz-security-token"));
	}

	@Test
	public void environment() {
		env("AKIAENV");
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("AKIAENV", refresh(provider));
		assertEquals("environment", provider.credentialSource());
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
		assertTrue(messages.toString(), messages.contains("INFO AWS EC2 seed provider credentials from environment: " +
			"access key AKIAENV"));
		assertNoSecret(messages.toString(), "");
	}

	@Test
	public void profileFileWithAwsProfile() throws Exception {
		env.set("AWS_SHARED_CREDENTIALS_FILE", profileFile("credentials").toString()).set("AWS_PROFILE", "seed");
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("AKIASEEDPROFILE", refresh(provider));
		assertEquals("profile", provider.credentialSource());
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
	}

	@Test
	public void profileFileDefaultInHome() throws Exception {
		Files.createDirectories(dir.resolve(".aws"));
		profileFile(".aws/credentials");
		assertEquals("AKIADEFAULT", refresh(builder().build()));
	}

	@Test
	public void profileWithoutStaticKeysIsNotInChain() throws Exception {
		env.set("AWS_SHARED_CREDENTIALS_FILE", profileFile("credentials").toString()).set("AWS_PROFILE", "sso-only");
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("ASIAIMDS", refresh(provider));
		assertEquals("imds", provider.credentialSource());
	}

	@Test
	public void webIdentityThroughStubSts() throws Exception {
		webIdentity();
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("ASIASTS", refresh(provider));
		assertEquals("web-identity", provider.credentialSource());

		Request sts = stub.requests().get(0);
		System.out.println("AWS-TAGS-STS " + sts.method + ' ' + sts.path + " Action=" + sts.params.get("Action") +
			" Version=" + sts.params.get("Version") + " RoleArn=" + sts.params.get("RoleArn") + " RoleSessionName=" +
			sts.params.get("RoleSessionName") + " WebIdentityToken=" + (WEB_TOKEN.equals(sts.params.get("WebIdentityToken"))?
			"<file contents>" : "<wrong>"));
		assertEquals("/sts", sts.path);
		assertEquals("AssumeRoleWithWebIdentity", sts.params.get("Action"));
		assertEquals("2011-06-15", sts.params.get("Version"));
		assertEquals(ROLE_ARN, sts.params.get("RoleArn"));
		assertEquals("seed-session", sts.params.get("RoleSessionName"));
		assertEquals(WEB_TOKEN, sts.params.get("WebIdentityToken"));
		assertNull(sts.headers.get("authorization"));
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
	}

	@Test
	public void webIdentityStsErrorHasCodeAndNoToken() throws Exception {
		webIdentity();
		stub.sts(new Reply(400, "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error>" +
			"<Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>Token " + WEB_TOKEN + " is invalid</Message>" +
			"</Error><RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId></ErrorResponse>"));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> builder().build().refreshSeedCandidates(INIT));
		System.out.println("AWS-TAGS-STS-ERROR " + ae.getMessage());
		assertTrue(ae.getMessage(), ae.getMessage().contains("STS AssumeRoleWithWebIdentity failed: HTTP 400 " +
			"InvalidIdentityToken: Token <redacted> is invalid (request id c6104cbe-af31-11e0-8154-cbc7ccf896c7)"));
		assertEquals(0, stub.count("/"));
	}

	@Test
	public void ecsWithAuthorizationToken() {
		env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", StubAwsServer.ECS_AUTHORIZATION);
		stub.credentials(StubAwsServer.credentialsJson("ASIAECS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("ASIAECS", refresh(provider));
		assertEquals("ecs", provider.credentialSource());

		Request ecs = stub.requests().get(0);
		System.out.println("AWS-TAGS-ECS " + ecs.method + ' ' + ecs.path + " authorization=" +
			StubAwsServer.ECS_AUTHORIZATION.equals(ecs.headers.get("authorization")));
		assertEquals("/ecs/v2/credentials/task-1", ecs.path);
	}

	@Test
	public void ecsFullUriWithTokenFile() throws Exception {
		Path token = dir.resolve("ecs-token");
		Files.write(token, StubAwsServer.ECS_AUTHORIZATION.getBytes(StandardCharsets.UTF_8));
		env.set("AWS_CONTAINER_CREDENTIALS_FULL_URI", stub.url() + "/ecs/full")
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", token.toString())
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", "wrong");
		stub.credentials(StubAwsServer.credentialsJson("ASIAECSFULL", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		assertEquals("ASIAECSFULL", refresh(builder().build()));
	}

	@Test
	public void ecsWithoutAuthorizationFails() {
		env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1");
		AerospikeException ae = assertThrows(AerospikeException.class, () -> builder().build().refreshSeedCandidates(INIT));
		System.out.println("AWS-TAGS-ECS-ERROR " + ae.getMessage());
		assertTrue(ae.getMessage(), ae.getMessage().contains("ECS credentials failed: HTTP 401"));
	}

	@Test
	public void imdsV2TokenHandshake() {
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("ASIAIMDS", refresh(provider));
		assertEquals("imds", provider.credentialSource());

		List<Request> requests = stub.requests();
		System.out.println("AWS-TAGS-IMDS " + requests.get(0).method + ' ' + requests.get(0).path + " ttl=" +
			requests.get(0).headers.get("x-aws-ec2-metadata-token-ttl-seconds") + ", " + requests.get(1).method + ' ' +
			requests.get(1).path + ", " + requests.get(2).method + ' ' + requests.get(2).path + " token=" +
			StubAwsServer.IMDS_TOKEN.equals(requests.get(2).headers.get("x-aws-ec2-metadata-token")));
		assertEquals("PUT", requests.get(0).method);
		assertEquals("/latest/api/token", requests.get(0).path);
		assertEquals("21600", requests.get(0).headers.get("x-aws-ec2-metadata-token-ttl-seconds"));
		assertEquals("/latest/meta-data/iam/security-credentials/", requests.get(1).path);
		assertEquals("/latest/meta-data/iam/security-credentials/" + StubAwsServer.IMDS_ROLE, requests.get(2).path);
		assertEquals(StubAwsServer.IMDS_TOKEN, requests.get(2).headers.get("x-aws-ec2-metadata-token"));
	}

	@Test
	public void noSourceFound() {
		env.set("AWS_EC2_METADATA_DISABLED", "true");
		AerospikeException ae = assertThrows(AerospikeException.class, () -> builder().build().refreshSeedCandidates(INIT));
		System.out.println("AWS-TAGS-NO-CREDENTIALS " + ae.getMessage());
		assertTrue(ae.getMessage(), ae.getMessage().contains("no AWS credentials found"));
		assertEquals(0, stub.requests().size());
	}

	@Test
	public void chainOrderHigherSourceWins() throws Exception {
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
			.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", StubAwsServer.ECS_AUTHORIZATION);
		assertEquals("ecs over imds", "ASIAIMDS", refresh(builder().build()));

		webIdentity();
		assertEquals("web identity over ecs", "ASIASTS", refresh(builder().build()));

		env.set("AWS_SHARED_CREDENTIALS_FILE", profileFile("credentials").toString()).set("AWS_PROFILE", "seed");
		assertEquals("profile over web identity", "AKIASEEDPROFILE", refresh(builder().build()));

		env("AKIAENV");
		assertEquals("environment over profile", "AKIAENV", refresh(builder().build()));
		assertEquals("config over environment", KEY, refresh(builder().credentials(KEY, SECRET, null).build()));
		assertEquals("hook over config", "AKIAHOOK",
			refresh(builder().credentialsHook(() -> new Credentials("AKIAHOOK", SECRET, null)).build()));
		assertEquals(1, stub.count("/sts"));
		assertEquals(0, stub.count("/latest/api/token"));
	}

	@Test
	public void expiryRefreshesNearExpiryAndNotBefore() {
		String expiration = Instant.ofEpochMilli(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)).toString();
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS1", SECRET, TOKEN, expiration));
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("ASIAIMDS1", refresh(provider));
		assertEquals(1, stub.count("/latest/api/token"));
		assertEquals(1, stub.count("/latest/meta-data/iam/security-credentials/"));

		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS2", SECRET, TOKEN, "2099-01-01T00:00:00Z"));
		env.clockOffset = TimeUnit.MINUTES.toMillis(54);
		assertEquals("6 minutes before expiry", "ASIAIMDS1", refresh(provider));
		assertEquals(1, stub.count("/latest/api/token"));

		env.clockOffset = TimeUnit.MINUTES.toMillis(56);
		assertEquals("4 minutes before expiry", "ASIAIMDS2", refresh(provider));
		System.out.println("AWS-TAGS-EXPIRY tokenRequests=" + stub.count("/latest/api/token") + " roleRequests=" +
			stub.count("/latest/meta-data/iam/security-credentials/"));
		assertEquals(2, stub.count("/latest/api/token"));
		assertEquals("role name cached", 1, stub.count("/latest/meta-data/iam/security-credentials/"));
	}

	@Test
	public void failedReloadKeepsUnexpiredCredentials() {
		String expiration = Instant.ofEpochMilli(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)).toString();
		stub.credentials(StubAwsServer.credentialsJson("ASIAIMDS1", SECRET, TOKEN, expiration));
		AwsEc2TagSeedCandidateProvider provider = builder().build();
		assertEquals("ASIAIMDS1", refresh(provider));

		stub.credentials("{\"Code\":\"Failure\"}");
		env.clockOffset = TimeUnit.MINUTES.toMillis(58);
		assertEquals("ASIAIMDS1", refresh(provider));
		assertTrue(messages.toString(), messages.stream().anyMatch(m -> m.startsWith("WARN AWS EC2 seed provider: " +
			"credentials refresh from imds failed, using cached credentials until they expire")));

		env.clockOffset = TimeUnit.MINUTES.toMillis(61);
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage(), ae.getMessage().contains("IMDS credentials failed: Failure"));
	}

	@Test
	public void hookReplacesChain() {
		env("AKIAENV");
		AtomicInteger calls = new AtomicInteger();
		AwsEc2TagSeedCandidateProvider provider = builder().credentialsHook(() -> {
			calls.incrementAndGet();
			return new Credentials("ASIAHOOK", SECRET, TOKEN,
				System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(calls.get() == 1? 4 : 60));
		}).build();

		assertEquals("ASIAHOOK", refresh(provider));
		assertEquals("hook", provider.credentialSource());
		assertEquals(TOKEN, lastEc2().headers.get("x-amz-security-token"));
		refresh(provider);
		refresh(provider);
		System.out.println("AWS-TAGS-HOOK calls=" + calls.get() + " after 3 refreshes (first expiry within margin)");
		assertEquals(2, calls.get());
	}

	@Test
	public void hookFailure() {
		AwsEc2TagSeedCandidateProvider provider = builder().credentialsHook(() -> null).build();
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage(), ae.getMessage().contains("credentials hook returned null"));

		AwsEc2TagSeedCandidateProvider throwing = builder().credentialsHook(() -> {
			throw new IllegalStateException(SECRET);
		}).build();
		ae = assertThrows(AerospikeException.class, () -> throwing.refreshSeedCandidates(INIT));
		assertTrue(ae.getMessage(), ae.getMessage().contains("credentials hook failed: java.lang.IllegalStateException"));
		assertNoSecret(ae.getMessage(), "");
	}

	@Test
	public void jsonParser() {
		assertEquals("{a=x\"y, n=12, b=true, s=é\n}", AwsCredentialChain.Json.object(
			"{\"a\":\"x\\\"y\", \"n\": 12, \"o\": {\"k\": [1, {\"z\": \"}\"}]}, \"b\":true, \"nil\": null, " +
			"\"s\": \"\\u00e9\\n\"}").toString());
		assertEquals("{}", AwsCredentialChain.Json.object(" { } ").toString());
		assertThrows(IllegalArgumentException.class, () -> AwsCredentialChain.Json.object("{\"a\" 1}"));
	}
}
