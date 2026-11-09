package sun.net.spi.nameservice;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Compile-time copy of the JDK 8 name service SPI interface, which {@code --release 8} does not
 * expose. On a Java 8 JVM the boot class path copy is loaded instead of this one.
 */
public interface NameService {
	InetAddress[] lookupAllHostAddr(String host) throws UnknownHostException;

	String getHostByAddr(byte[] addr) throws UnknownHostException;
}
