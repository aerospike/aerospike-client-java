package com.aerospike.client.crypto;


import com.aerospike.client.Value;

public interface CryptoProvider {

    public byte[] computeDigest(String setName, Value key);

    /**
     * Decode base64 bytes into a byte array.
     */
    public byte[] decodeBase64(byte[] src, int off, int len);

    /**
     * Encode bytes into a base64 encoded string.
     */
    public  String encodeBase64(byte[] src);
}
