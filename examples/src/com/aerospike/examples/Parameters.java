package com.aerospike.examples;

import net.citrusleaf.CitrusleafInfo;

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
	

	protected Parameters(String host, int port, String namespace, String set) {
		this.host = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	
	/**
	 * Some database calls need to know whether the server is configured as
	 * multi-bin or single-bin.
	 */
	protected void setIsSingleBin() throws Exception {
		String filter = "namespace/" + namespace;
		String tokens = CitrusleafInfo.get(host, port, filter);

		if (tokens == null) {
			throw new Exception(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				host, port, namespace));
		}

		String name = "single-bin";
		String search = name + '=';
		int begin = tokens.indexOf(search);

		if (begin < 0) {
			throw new Exception(String.format(
				"Failed to find namespace attribute: host=%s port=%d namespace=%s attribute=%s",
				host, port, namespace, name));
		}

		begin += search.length();
		int end = tokens.indexOf(';', begin);

		if (end < 0) {
			end = tokens.length();
		}

		String value = tokens.substring(begin, end);
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
