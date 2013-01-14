package com.aerospike.examples;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;

public class ServerInfo extends Example {

	public ServerInfo(Console console) {
		super(console);
	}

	/**
	 * Query server configuration, cluster status and namespace configuration.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		GetServerConfig(params);
		console.write("");
		GetNamespaceConfig(params);
	}

	/**
	 * Query server configuration and cluster status.
	 */
	private void GetServerConfig(Parameters params) throws Exception {
		console.write("Server Configuration");
		Map<String,String> map = Info.request(params.host, params.port);

		if (map == null) {
			throw new Exception(String.format("Failed to get server info: host=%s port=%d",
				params.host, params.port));
		}

		for (Map.Entry<String,String> entry : map.entrySet()) {
			if (entry.getKey().equals("statistics")) {
				LogNameValueTokens(entry.getValue());
			}
			else {
				console.write(entry.getKey() + '=' + entry.getValue());
			}
		}
	}

	/**
	 * Query namespace configuration.
	 */
	private void GetNamespaceConfig(Parameters params) throws Exception {
		console.write("Namespace Configuration");
		String filter = "namespace/" + params.namespace;
		String tokens = Info.request(params.host, params.port, filter);

		if (tokens == null) {
			throw new Exception(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				params.host, params.port, params.namespace));
		}

		LogNameValueTokens(tokens);
	}

	private void LogNameValueTokens(String tokens) {
		String[] values = tokens.split(";");

		for (String value : values) {
			console.write(value);
		}
	}
}
