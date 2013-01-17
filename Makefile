# Make Java Client

.PHONY: default
default:
	cd client ; mvn install
	cd examples ; mvn package
	cd benchmarks ; mvn package
	cd servlets ; mvn package

.PHONY: clean
clean: 
	cd client ; mvn clean
	cd examples ; mvn clean
	cd benchmarks ; mvn clean
	cd servlets ; mvn clean

.PHONY: package
package:
	$(MAKE) -f package/Makefile
