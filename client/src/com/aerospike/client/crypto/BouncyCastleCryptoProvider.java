package com.aerospike.client.crypto;


import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import org.bouncycastle.crypto.digests.RIPEMD160Digest;

import java.util.Base64;

public class BouncyCastleCryptoProvider implements CryptoProvider {
    /**
     * Generate unique server hash value from set name, key type and user defined key.
     * The hash function is RIPEMD-160 (a 160 bit hash).
     */
    @Override
    public byte[] computeDigest(String setName, Value key) {
        int size = Buffer.estimateSizeUtf8Quick(setName) + 1 + key.estimateKeySize();
        byte[] buffer = new byte[size];
        int setLength = Buffer.stringToUtf8(setName, buffer, 0);

        buffer[setLength] = (byte)key.getType();
        int keyLength = key.write(buffer, setLength + 1);

        RIPEMD160Digest hash = new RIPEMD160Digest();
        hash.update(buffer, 0, setLength);
        hash.update(buffer, setLength, keyLength + 1);

        byte[] digest = new byte[20];
        hash.doFinal(digest, 0);
        return digest;
    }

    /**
     * Decode base64 bytes into a byte array.
     */
    public  byte[] decodeBase64(byte[] src, int off, int len) {
        Base64.Decoder decoder = Base64.getDecoder();
        return decoder.decode(new String(src, off, len));
    }

    /**
     * Encode bytes into a base64 encoded string.
     */
    public  String encodeBase64(byte[] src) {
        Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encodeToString(src);
    }
}
