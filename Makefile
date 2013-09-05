# Make Java Client

.PHONY: default
default:
	cd client ; mvn install
	cd examples ; mvn package
	cd benchmarks ; mvn package
	cd servlets ; mvn package

.PHONY: configure
configure: $(HOME)/.m2/repository/org/gnu/gnu-crypto/2.0.1/gnu-crypto-2.0.1.jar

$(HOME)/.m2/repository/org/gnu/gnu-crypto/2.0.1/gnu-crypto-2.0.1.jar:
	mvn install:install-file -Dfile=client/depends/gnu-crypto.jar -DgroupId=org.gnu -DartifactId=gnu-crypto -Dversion=2.0.1 -Dpackaging=jar

.PHONY: clean
clean: 
	cd client ; mvn clean
	cd examples ; mvn clean
	cd benchmarks ; mvn clean
	cd servlets ; mvn clean

.PHONY: package
package:
	$(MAKE) -f package/Makefile
