package com.aerospike.client.crypto;

import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import gnu.crypto.hash.RipeMD160;
import gnu.crypto.util.Base64;

public class GnuCryptoProvider implements CryptoProvider {
    @Override
    public byte[] computeDigest(String setName, Value key) {
        int size = Buffer.estimateSizeUtf8Quick(setName) + 1 + key.estimateKeySize();
        byte[] buffer = new byte[size];
        int setLength = Buffer.stringToUtf8(setName, buffer, 0);

        buffer[setLength] = (byte)key.getType();
        int keyLength = key.write(buffer, setLength + 1);

        RipeMD160 hash = new RipeMD160();
        hash.update(buffer, 0, setLength);
        hash.update(buffer, setLength, keyLength + 1);
        return hash.digest();
    }

    @Override
    public byte[] decodeBase64(byte[] src, int off, int len) {
        return Base64.decode(src, off, len);
    }

    @Override
    public String encodeBase64(byte[] src) {
        return Base64.encode(src, 0, src.length, false);
    }
}
