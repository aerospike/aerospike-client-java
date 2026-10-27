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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.w3c.dom.Element;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.configuration.serializers.staticconfig.StaticDiscoveryConfig;

/**
 * Seed candidate provider that lists running EC2 instances matching a set of filters, usually a tag,
 * through the EC2 {@code DescribeInstances} API, and returns one seed host per instance. Use it for
 * a self-managed Aerospike cluster on EC2 whose nodes carry a known tag.
 * <p>
 * Each refresh sends one SigV4-signed EC2 Query API {@code DescribeInstances} request (API version
 * 2016-11-15) with {@link Builder#filters} plus {@code instance-state-name=running}, and follows
 * {@code NextToken} pagination. It returns one host per instance with a non-empty
 * {@link Builder#addressField}, with {@link Builder#port} and {@link Builder#tlsName}, deduplicated
 * and sorted by address. An instance without that field is skipped and the count is logged at debug
 * level. Any API, credential or timeout failure throws an {@link AerospikeException} with the AWS
 * error code and request id when there is one, so the client keeps the previous seed list. No access
 * key secret, session token, web identity token or signature is logged or put in a message.
 * <p>
 * Minimum IAM policy of the credentials the provider uses:
 * <pre>{@code
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {"Effect": "Allow", "Action": ["ec2:DescribeInstances"], "Resource": "*"}
 *   ]
 * }
 * }</pre>
 * <p>
 * Credentials: {@link Builder#credentialsHook}, when set, replaces the chain below. Otherwise the
 * first source found, in this order, is used:
 * <ol>
 * <li>{@link Builder#credentials} keys;</li>
 * <li>environment variables {@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY} and
 * {@code AWS_SESSION_TOKEN};</li>
 * <li>static keys of profile {@code AWS_PROFILE} (default {@code default}) in the shared
 * credentials file {@code AWS_SHARED_CREDENTIALS_FILE} (default {@code ~/.aws/credentials});</li>
 * <li>EKS web identity: {@code AWS_WEB_IDENTITY_TOKEN_FILE} and {@code AWS_ROLE_ARN}, through STS
 * {@code AssumeRoleWithWebIdentity} on the regional STS endpoint;</li>
 * <li>ECS container credentials: {@code AWS_CONTAINER_CREDENTIALS_RELATIVE_URI} or
 * {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}, with {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE} or
 * {@code AWS_CONTAINER_AUTHORIZATION_TOKEN};</li>
 * <li>EC2 instance profile through IMDSv2 (session token, then the role credentials), unless
 * {@code AWS_EC2_METADATA_DISABLED} is {@code true}.</li>
 * </ol>
 * SSO, {@code credential_process}, {@code role_arn} profiles and cross-account roles are not in the
 * chain; use the hook, which may wrap the application's own AWS SDK credentials provider. The chain
 * resolves once, at the first refresh; later refreshes reuse the source that answered. Credentials
 * are cached with their expiry and reloaded on the refreshing thread when they are within five
 * minutes of expiry, never on a timer.
 * <p>
 * Threads and deadlines: the Java client runs discovery on the cluster tend thread only. The
 * provider needs periodic refresh, so {@link Builder#timeout} must not exceed
 * {@link com.aerospike.client.policy.ClientPolicy#discoveryTendRefreshDeadline}; client
 * construction fails otherwise. Every connect and read of one tend refresh, credentials included,
 * is bounded by the time left of {@link Builder#timeout}, and the first refresh, which runs in the
 * client constructor, by {@link Builder#initTimeout}. Endpoint host names are resolved by the system
 * resolver, which the provider cannot bound. The provider starts no thread and uses no HTTP proxy.
 * For an in-region endpoint, the defaults ({@code timeout} 80ms, {@code discoveryTendRefreshDeadline}
 * 100ms) leave room for one call. An out-of-region endpoint, or a slow credential reload on tend,
 * overruns the deadline; after three consecutive overruns the client disables the provider and
 * restores the initial seeds.
 * <p>
 * The provider sees only the instances of the credentials' own account, so it serves self-managed
 * clusters on EC2, not Aerospike Cloud.
 * <p>
 * Example, with a static seed kept as a fallback:
 * <pre>{@code
 * ClientPolicy policy = new ClientPolicy();
 * policy.seedCandidateProvider = AwsEc2TagSeedCandidateProvider.builder()
 *     .region("us-east-1")
 *     .tagFilter("aerospike-cluster", "prod-a")
 *     .addressField(AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_IP)
 *     .port(3000)
 *     .build();
 * policy.discoveryMergePolicy = SeedMergePolicy.MERGE;
 * AerospikeClient client = new AerospikeClient(policy, new Host("10.0.1.10", 3000));
 * }</pre>
 */
public final class AwsEc2TagSeedCandidateProvider implements SeedCandidateProvider {
	/**
	 * Instance field returned as the seed address.
	 */
	public enum AddressField {
		/**
		 * Primary private IPv4 address ({@code privateIpAddress}).
		 */
		PRIVATE_IP("privateIpAddress"),

		/**
		 * Public IPv4 address ({@code ipAddress}).
		 */
		PUBLIC_IP("ipAddress"),

		/**
		 * Private DNS name ({@code privateDnsName}).
		 */
		PRIVATE_DNS("privateDnsName"),

		/**
		 * Public DNS name ({@code dnsName}).
		 */
		PUBLIC_DNS("dnsName");

		final String element;

		AddressField(String element) {
			this.element = element;
		}
	}

	/**
	 * AWS credentials used to sign requests. {@link #toString()} shows only the access key id.
	 */
	public static final class Credentials {
		private final String accessKeyId;
		private final String secretAccessKey;
		private final String sessionToken;
		private final long expiration;

		/**
		 * Credentials without expiry.
		 *
		 * @param accessKeyId		access key id, required
		 * @param secretAccessKey	secret access key, required
		 * @param sessionToken		session token of temporary credentials, may be null
		 */
		public Credentials(String accessKeyId, String secretAccessKey, String sessionToken) {
			this(accessKeyId, secretAccessKey, sessionToken, 0);
		}

		/**
		 * Credentials with an expiry.
		 *
		 * @param accessKeyId		access key id, required
		 * @param secretAccessKey	secret access key, required
		 * @param sessionToken		session token of temporary credentials, may be null
		 * @param expiration		expiry in milliseconds since the epoch, or 0 for none
		 * @throws AerospikeException	if the access key id or secret is null or empty
		 */
		public Credentials(String accessKeyId, String secretAccessKey, String sessionToken, long expiration) {
			if (accessKeyId == null || accessKeyId.isEmpty()) {
				throw new AerospikeException("Invalid AWS EC2 seed provider credentials: access key id is empty");
			}
			if (secretAccessKey == null || secretAccessKey.isEmpty()) {
				throw new AerospikeException("Invalid AWS EC2 seed provider credentials: secret access key is empty");
			}
			this.accessKeyId = accessKeyId;
			this.secretAccessKey = secretAccessKey;
			this.sessionToken = (sessionToken == null || sessionToken.isEmpty())? null : sessionToken;
			this.expiration = expiration;
		}

		/**
		 * Return the access key id.
		 */
		public String getAccessKeyId() {
			return accessKeyId;
		}

		/**
		 * Return the secret access key.
		 */
		public String getSecretAccessKey() {
			return secretAccessKey;
		}

		/**
		 * Return the session token. May be null.
		 */
		public String getSessionToken() {
			return sessionToken;
		}

		/**
		 * Return the expiry in milliseconds since the epoch, or 0 for none.
		 */
		public long getExpiration() {
			return expiration;
		}

		@Override
		public String toString() {
			return "Credentials{accessKeyId=" + accessKeyId + ", expiration=" + expiration + '}';
		}
	}

	/**
	 * Source of credentials that replaces the built-in credential chain.
	 * <p>
	 * Called on the thread that refreshes the provider: the application thread that constructs the
	 * client for the first refresh, then the cluster tend thread. It must return quickly, well within
	 * {@link Builder#timeout}; an implementation that does I/O should cache. It may wrap the
	 * application's own AWS SDK credentials provider, which caches and refreshes its credentials.
	 * Credentials returned with an expiry are cached until five minutes before it; credentials without
	 * one are used for the life of the provider.
	 */
	@FunctionalInterface
	public interface CredentialsHook {
		/**
		 * Return the credentials. Must not return null.
		 */
		Credentials get();
	}

	/**
	 * Default port set on every host.
	 */
	public static final int DEFAULT_PORT = 3000;

	/**
	 * Default tend refresh timeout in milliseconds.
	 */
	public static final int DEFAULT_TIMEOUT = 80;

	/**
	 * Default first refresh timeout in milliseconds.
	 */
	public static final int DEFAULT_INIT_TIMEOUT = 10000;

	static final String API_VERSION = "2016-11-15";
	static final String STATE_FILTER = "instance-state-name";

	private static final String FORM = "application/x-www-form-urlencoded; charset=utf-8";
	private static final String SERVICE = "ec2";

	private final String region;
	private final Map<String,List<String>> filters;
	private final AddressField addressField;
	private final int port;
	private final String tlsName;
	private final int timeout;
	private final int initTimeout;
	private final String endpoint;
	private final URI uri;
	private final String hostHeader;
	private final String path;
	private final String query;
	private final AwsEnvironment env;
	private final AwsXml xml;
	private final AwsCredentialChain chain;

	/**
	 * Return a builder.
	 */
	public static Builder builder() {
		return new Builder();
	}

	private AwsEc2TagSeedCandidateProvider(Builder builder) {
		if (builder.region == null || ! builder.region.matches("[a-z0-9]+(-[a-z0-9]+)+")) {
			throw new AerospikeException("Invalid AWS EC2 seed provider region: '" + builder.region + "'");
		}

		if (builder.filters.isEmpty()) {
			throw new AerospikeException("Invalid AWS EC2 seed provider filters: at least one filter is required");
		}

		for (Map.Entry<String,List<String>> entry : builder.filters.entrySet()) {
			String name = entry.getKey();

			if (name == null || name.isEmpty()) {
				throw new AerospikeException("Invalid AWS EC2 seed provider filter name: '" + name + "'");
			}
			if (name.equals(STATE_FILTER)) {
				throw new AerospikeException("Invalid AWS EC2 seed provider filter name: '" + name +
					"': the provider adds " + STATE_FILTER + "=running");
			}

			List<String> values = entry.getValue();

			if (values == null || values.isEmpty()) {
				throw new AerospikeException("Invalid AWS EC2 seed provider filter '" + name + "' values: empty");
			}

			for (String value : values) {
				if (value == null || value.isEmpty()) {
					throw new AerospikeException("Invalid AWS EC2 seed provider filter '" + name + "' value: '" +
						value + "'");
				}
			}
		}

		if (builder.addressField == null) {
			throw new AerospikeException("Invalid AWS EC2 seed provider addressField: null");
		}

		if (builder.port <= 0 || builder.port > 65535) {
			throw new AerospikeException("Invalid AWS EC2 seed provider port: " + builder.port);
		}

		if (builder.tlsName != null && builder.tlsName.isEmpty()) {
			throw new AerospikeException("Invalid AWS EC2 seed provider tlsName: empty");
		}

		if (builder.timeout <= 0) {
			throw new AerospikeException("Invalid AWS EC2 seed provider timeout: " + builder.timeout);
		}

		if (builder.initTimeout <= 0) {
			throw new AerospikeException("Invalid AWS EC2 seed provider initTimeout: " + builder.initTimeout);
		}

		if (builder.credentials != null && builder.credentialsHook != null) {
			throw new AerospikeException("Invalid AWS EC2 seed provider credentials: credentials and credentialsHook " +
				"cannot both be set");
		}

		this.env = builder.env;
		String target = (builder.endpoint != null)? builder.endpoint : env.ec2Endpoint(builder.region);

		try {
			this.uri = new URI(target);
		}
		catch (URISyntaxException e) {
			throw new AerospikeException("Invalid AWS EC2 seed provider endpoint: '" + target + "'");
		}

		if (uri.getHost() == null || uri.getRawQuery() != null || uri.getRawFragment() != null ||
			! ("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))) {
			throw new AerospikeException("Invalid AWS EC2 seed provider endpoint: '" + target +
				"': expected http or https URL with a host and no query");
		}

		this.region = builder.region;
		this.filters = Collections.unmodifiableMap(copy(builder.filters));
		this.addressField = builder.addressField;
		this.port = builder.port;
		this.tlsName = builder.tlsName;
		this.timeout = builder.timeout;
		this.initTimeout = builder.initTimeout;
		this.endpoint = builder.endpoint;
		this.hostHeader = AwsHttp.host(uri);
		this.path = (uri.getRawPath() == null || uri.getRawPath().isEmpty())? "/" : uri.getRawPath();
		this.query = query(filters);
		this.xml = new AwsXml();
		this.chain = new AwsCredentialChain(builder.credentials, builder.credentialsHook, env, region, xml);
	}

	private static Map<String,List<String>> copy(Map<String,List<String>> filters) {
		Map<String,List<String>> map = new LinkedHashMap<>();

		for (Map.Entry<String,List<String>> entry : filters.entrySet()) {
			map.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
		}
		return map;
	}

	private static String query(Map<String,List<String>> filters) {
		StringBuilder sb = new StringBuilder("Action=DescribeInstances&Version=").append(API_VERSION);
		int n = 0;

		for (Map.Entry<String,List<String>> entry : filters.entrySet()) {
			append(sb, ++n, entry.getKey(), entry.getValue());
		}
		append(sb, ++n, STATE_FILTER, Collections.singletonList("running"));
		return sb.toString();
	}

	private static void append(StringBuilder sb, int n, String name, List<String> values) {
		sb.append("&Filter.").append(n).append(".Name=").append(AwsSigV4.encode(name));
		int v = 0;

		for (String value : values) {
			sb.append("&Filter.").append(n).append(".Value.").append(++v).append('=').append(AwsSigV4.encode(value));
		}
	}

	/**
	 * List the running instances matching the filters and return one host per instance. The first
	 * call, at client initialization, is bounded by {@link #getInitTimeout()}; later calls by
	 * {@link #getTimeout()}.
	 *
	 * @throws AerospikeException	on any API, credential or timeout failure
	 */
	@Override
	public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
		int budget = (ctx.tendIteration == 0)? initTimeout : timeout;
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budget);
		Credentials credentials = chain.get(deadline);
		TreeSet<String> addresses = new TreeSet<>();
		String nextToken = null;
		int instances = 0;
		int skipped = 0;
		int pages = 0;

		do {
			Element root = describe(credentials, nextToken, deadline, budget);
			pages++;

			for (Element reservation : AwsXml.children(AwsXml.child(root, "reservationSet"), "item")) {
				for (Element instance : AwsXml.children(AwsXml.child(reservation, "instancesSet"), "item")) {
					instances++;
					String address = AwsXml.text(instance, addressField.element);

					if (address == null || address.isEmpty()) {
						skipped++;
					}
					else {
						addresses.add(address);
					}
				}
			}
			nextToken = AwsXml.text(root, "nextToken");
		} while (nextToken != null && ! nextToken.isEmpty());

		List<Host> hosts = new ArrayList<>(addresses.size());

		for (String address : addresses) {
			hosts.add(new Host(address, tlsName, port));
		}

		if (Log.debugEnabled()) {
			if (skipped > 0) {
				Log.debug("AWS EC2 seed provider skipped " + skipped + " instances without " + addressField.element);
			}
			Log.debug("AWS EC2 seed provider " + region + " returned " + hosts + " from " + instances +
				" instances in " + pages + " pages");
		}
		return hosts;
	}

	private Element describe(Credentials credentials, String nextToken, long deadline, int budget) {
		String body = (nextToken == null)? query : query + "&NextToken=" + AwsSigV4.encode(nextToken);
		byte[] payload = body.getBytes(StandardCharsets.UTF_8);
		String amzDate = AwsSigV4.amzDate(env.currentTimeMillis());
		TreeMap<String,String> headers = new TreeMap<>();
		headers.put("content-type", FORM);
		headers.put("host", hostHeader);
		headers.put("x-amz-date", amzDate);

		if (credentials.getSessionToken() != null) {
			headers.put("x-amz-security-token", credentials.getSessionToken());
		}

		String authorization = AwsSigV4.authorization(credentials.getAccessKeyId(),
			credentials.getSecretAccessKey(), "POST", path, "", headers, payload, region, SERVICE, amzDate);
		String signature = authorization.substring(authorization.lastIndexOf('=') + 1);
		headers.put("authorization", authorization);

		AwsHttp.Response response;

		try {
			response = AwsHttp.request("POST", uri, headers, payload, deadline);
		}
		catch (SocketTimeoutException e) {
			throw new AerospikeException("AWS EC2 seed provider: DescribeInstances timed out after " +
				budget + "ms");
		}
		catch (IOException e) {
			throw new AerospikeException("AWS EC2 seed provider: DescribeInstances failed: " +
				AwsXml.redact(e.toString(), credentials.getSecretAccessKey(), credentials.getSessionToken(), signature));
		}

		if (response.status != 200) {
			throw xml.error("DescribeInstances", response, credentials.getSecretAccessKey(),
				credentials.getSessionToken(), signature);
		}

		try {
			return xml.parse(response.body);
		}
		catch (Exception e) {
			throw new AerospikeException("AWS EC2 seed provider: invalid DescribeInstances response: " + e.getMessage());
		}
	}

	String credentialSource() {
		AwsCredentialChain.Source source = chain.source();
		return (source == null)? null : source.label;
	}

	/**
	 * Return the AWS region.
	 */
	public String getRegion() {
		return region;
	}

	/**
	 * Return the filters, by name, in configured order. Unmodifiable.
	 */
	public Map<String,List<String>> getFilters() {
		return filters;
	}

	/**
	 * Return the instance field returned as the seed address.
	 */
	public AddressField getAddressField() {
		return addressField;
	}

	/**
	 * Return the port set on every host.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Return the TLS name set on every host. May be null.
	 */
	public String getTlsName() {
		return tlsName;
	}

	/**
	 * Return the total timeout in milliseconds for one tend refresh, credentials included.
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Return the total timeout in milliseconds for the first refresh, credentials included.
	 */
	public int getInitTimeout() {
		return initTimeout;
	}

	/**
	 * Return the configured EC2 endpoint, or null for the regional endpoint.
	 */
	public String getEndpoint() {
		return endpoint;
	}

	/**
	 * Return true. Instances can change.
	 */
	@Override
	public boolean needsPeriodicRefresh() {
		return true;
	}

	/**
	 * Return true. All I/O of one refresh is bounded by {@link #getTimeout()}, or
	 * {@link #getInitTimeout()} for the first.
	 */
	@Override
	public boolean supportsDeadline() {
		return true;
	}

	@Override
	public String toString() {
		return StaticDiscoveryConfig.AWS_TAGS + ' ' + region + ' ' + filters;
	}

	/**
	 * Builder for {@link AwsEc2TagSeedCandidateProvider}. Values are validated by {@link #build()}.
	 */
	public static final class Builder {
		private String region;
		private final Map<String,List<String>> filters = new LinkedHashMap<>();
		private AddressField addressField = AddressField.PRIVATE_IP;
		private int port = DEFAULT_PORT;
		private String tlsName;
		private int timeout = DEFAULT_TIMEOUT;
		private int initTimeout = DEFAULT_INIT_TIMEOUT;
		private String endpoint;
		private Credentials credentials;
		private CredentialsHook credentialsHook;
		private AwsEnvironment env = AwsEnvironment.SYSTEM;

		private Builder() {
		}

		/**
		 * AWS region of the instances and of the EC2 and STS endpoints, such as {@code us-east-1}.
		 * Required.
		 */
		public Builder region(String region) {
			this.region = region;
			return this;
		}

		/**
		 * Add a {@code DescribeInstances} filter, passed to EC2 as given: {@code tag:<key>} or any
		 * other EC2 filter name. An instance matches when it matches one of the values of every
		 * filter. Adding a name again replaces its values. {@code instance-state-name=running} is
		 * always added and cannot be set. At least one filter is required.
		 */
		public Builder filter(String name, String... values) {
			filters.put(name, (values != null)? Arrays.asList(values.clone()) : null);
			return this;
		}

		/**
		 * Add a {@code DescribeInstances} filter. See {@link #filter(String, String...)}.
		 */
		public Builder filter(String name, List<String> values) {
			filters.put(name, (values != null)? new ArrayList<>(values) : null);
			return this;
		}

		/**
		 * Add filters by name. See {@link #filter(String, String...)}.
		 */
		public Builder filters(Map<String,List<String>> filters) {
			if (filters != null) {
				for (Map.Entry<String,List<String>> entry : filters.entrySet()) {
					filter(entry.getKey(), entry.getValue());
				}
			}
			return this;
		}

		/**
		 * Add filter {@code tag:<key>} with the given values.
		 */
		public Builder tagFilter(String key, String... values) {
			return filter("tag:" + key, values);
		}

		/**
		 * Instance field returned as the seed address.
		 * <p>
		 * Default: {@link AddressField#PRIVATE_IP}
		 */
		public Builder addressField(AddressField addressField) {
			this.addressField = addressField;
			return this;
		}

		/**
		 * Port set on every host.
		 * <p>
		 * Default: {@link AwsEc2TagSeedCandidateProvider#DEFAULT_PORT}
		 */
		public Builder port(int port) {
			this.port = port;
			return this;
		}

		/**
		 * TLS name set on every host. Null leaves the TLS name to be defaulted the same way as for
		 * configured seeds.
		 * <p>
		 * Default: null
		 */
		public Builder tlsName(String tlsName) {
			this.tlsName = tlsName;
			return this;
		}

		/**
		 * Total time in milliseconds for one tend refresh, credentials and every page included. Must
		 * be greater than zero, and must not exceed
		 * {@link com.aerospike.client.policy.ClientPolicy#discoveryTendRefreshDeadline}; the latter is
		 * checked at client construction.
		 * <p>
		 * Default: {@link AwsEc2TagSeedCandidateProvider#DEFAULT_TIMEOUT}
		 */
		public Builder timeout(int timeout) {
			this.timeout = timeout;
			return this;
		}

		/**
		 * Total time in milliseconds for the first refresh, which runs in the client constructor and
		 * resolves the credential chain. Must be greater than zero.
		 * <p>
		 * Default: {@link AwsEc2TagSeedCandidateProvider#DEFAULT_INIT_TIMEOUT}
		 */
		public Builder initTimeout(int initTimeout) {
			this.initTimeout = initTimeout;
			return this;
		}

		/**
		 * EC2 endpoint URL, such as a VPC interface endpoint. Null uses the regional endpoint
		 * {@code https://ec2.<region>.amazonaws.com}.
		 * <p>
		 * Default: null
		 */
		public Builder endpoint(String endpoint) {
			this.endpoint = endpoint;
			return this;
		}

		/**
		 * Static credentials, the first source of the credential chain. Cannot be combined with
		 * {@link #credentialsHook}.
		 * <p>
		 * Default: null
		 *
		 * @param accessKeyId		access key id
		 * @param secretAccessKey	secret access key
		 * @param sessionToken		session token of temporary credentials, may be null
		 * @throws AerospikeException	if the access key id or secret is null or empty
		 */
		public Builder credentials(String accessKeyId, String secretAccessKey, String sessionToken) {
			this.credentials = new Credentials(accessKeyId, secretAccessKey, sessionToken);
			return this;
		}

		/**
		 * Credentials hook that replaces the built-in credential chain. See {@link CredentialsHook}.
		 * Cannot be combined with {@link #credentials}.
		 * <p>
		 * Default: null
		 */
		public Builder credentialsHook(CredentialsHook credentialsHook) {
			this.credentialsHook = credentialsHook;
			return this;
		}

		Builder environment(AwsEnvironment env) {
			this.env = env;
			return this;
		}

		/**
		 * Validate the settings and build the provider.
		 *
		 * @throws AerospikeException	if a setting is invalid. The message names the setting and value.
		 */
		public AwsEc2TagSeedCandidateProvider build() {
			return new AwsEc2TagSeedCandidateProvider(this);
		}
	}
}
