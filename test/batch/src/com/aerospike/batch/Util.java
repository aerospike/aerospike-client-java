package com.aerospike.batch;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;

public final class Util {
	
	public static void batchInsertTestData(Parameters params) throws AerospikeException {
		if (params.verbose)
			System.out.println("Inserting " + params.numKeys + " keys....");
		
		for (Key key : params.keys) {
			insertKey(params, key);
		}
		
		if (params.verbose)
		  System.out.println("\tOK");
	}

	public static void insertKey(Parameters params, Key key) throws AerospikeException {
		AerospikeClient client = params.client;

		if (params.verbose)
			System.out.println("Setting key \"" + key.userKey + "\"....");
		
		client.put(null, key, new Bin(params.bin, key.userKey));
	}

    public static boolean generateKeys(Parameters params) throws AerospikeException {
		if (params.verbose)
		  System.out.println("Generating " + params.numKeys + " keys....");
		
		params.keys = new Key[params.numKeys];
		int max = params.startKey + params.numKeys;
		int count = 0;
		
		for (int i = params.startKey; i < max; i++) {
			String k = Util.genKey(i, params.keyLength);
			Key key = new Key(params.namespace, params.set, k);
			params.keys[count++] = key;
			if (params.verbose) {
				System.out.println("Key: \"" + k + "\"");
				String digestString = Buffer.bytesToHexString(key.digest);
				System.out.println("Digest: \"" + digestString + "\"");
			}
		}
		if (params.verbose)
		  System.out.println("\tOK");

		return true;
	}

    public static String genKey(int i, int keyLen)
    {
        String key = "";
        for(int j=keyLen-1; j>=0; j--) {
            key = (i % 10) + key;
            i /= 10;
        }
        return key;
    }
}
