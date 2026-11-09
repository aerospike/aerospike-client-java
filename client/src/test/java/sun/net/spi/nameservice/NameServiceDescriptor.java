package sun.net.spi.nameservice;

/**
 * Compile-time copy of the JDK 8 name service SPI interface, which {@code --release 8} does not
 * expose. On a Java 8 JVM the boot class path copy is loaded instead of this one.
 */
public interface NameServiceDescriptor {
	NameService createNameService() throws Exception;

	String getProviderName();

	String getType();
}
