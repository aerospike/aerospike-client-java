package com.aerospike.examples;

import java.util.Map;

import com.aerospike.client.Info;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Configuration data.
 */
public class Parameters {
	String host;
	int port;
	String namespace;
	String set;
	WritePolicy writePolicy;
	Policy policy;
	boolean singleBin;
	boolean isVersion3;
	
	protected Parameters(String host, int port, String namespace, String set) {
		this.host = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	
	/**
	 * Some database calls need to know how the server is configured.
	 */
	protected void setServerSpecific() throws Exception {
		String versionFilter = "version";
		String namespaceFilter = "namespace/" + namespace;
		Map<String,String> tokens = Info.request(host, port, versionFilter, namespaceFilter);

		String version = tokens.get(versionFilter);
		
		if (version != null && ! version.equals("Aerospike 2.0")) {
			isVersion3 = true;
		}
		
		String namespaceTokens = tokens.get(namespaceFilter);
		
		if (namespaceTokens == null) {
			throw new Exception(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				host, port, namespace));
		}

		String name = "single-bin";
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			throw new Exception(String.format(
				"Failed to find namespace attribute: host=%s port=%d namespace=%s attribute=%s",
				host, port, namespace, name));
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}

		String value = namespaceTokens.substring(begin, end);
		singleBin = Boolean.parseBoolean(value);
	}

	@Override
	public String toString() {
		return "Parameters: host=" + host + 
				" port=" + port + 
				" ns=" + namespace + 
				" set=" + set +
				" single-bin=" + singleBin;
	}

	public String getBinName(String name)
	{
		// Single bin servers don't need a bin name.
		return singleBin ? "" : name;
	}
}
