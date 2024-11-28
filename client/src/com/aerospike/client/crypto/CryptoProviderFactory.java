package com.aerospike.client.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CryptoProviderFactory {
    private final static String CRYPTO_PROVIDER = "crypto.algo";
    private static final Properties prop = new Properties();

    static {
        try (InputStream inputStream = CryptoProviderFactory.class.getClassLoader().getResourceAsStream("config.properties")) {
            prop.load(inputStream);
        } catch (IOException e) {
            // Not doing anyting since we are setting default value and crypto.algo to GNU
        }
    }

    public static CryptoProvider getProvider() {
        // Default is set to GNU if we do not get valid value in the config.properties.
        // Valid value would be -bc for bouncycastle and any other value for default aka GNU
        String provider = prop.getProperty(CRYPTO_PROVIDER, "");

        return "-bc".equalsIgnoreCase(provider) ? new BouncyCastleCryptoProvider() : new GnuCryptoProvider() ;
    }
}
