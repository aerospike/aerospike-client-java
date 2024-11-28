package com.aerospike.client.crypto;

import com.aerospike.client.Value;

public class Crypto {
    private static final CryptoProvider cryptoProvider = CryptoProviderFactory.getProvider();

    private Crypto() {
        throw new UnsupportedOperationException();
    }

    public static byte[] computeDigest(String  setName, Value key) {
        return cryptoProvider.computeDigest(setName, key);
    }

    public static byte[] decodeBase64(byte[] src, int off, int len) {
        return cryptoProvider.decodeBase64(src, off, len);
    }

    public static String encodeBase64(byte[] src) {
        return cryptoProvider.encodeBase64(src);
    }
}
