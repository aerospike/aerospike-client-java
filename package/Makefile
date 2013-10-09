# Package Java Client

SHELL=bash
REV=$(shell scripts/version)
NAME=aerospike-client-java-$(REV)
TMP_DIR=/tmp/$(NAME)

.PHONY: default
default:
	rm -rf package/aerospike-client-*
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	mkdir -p $(TMP_DIR)/client
	mkdir -p $(TMP_DIR)/examples
	mkdir -p $(TMP_DIR)/benchmarks
	mkdir -p $(TMP_DIR)/servlets
	cp pom.xml $(TMP_DIR)
	cp package/{README.md,build_all,license.txt} $(TMP_DIR)
	cp -r client/{README,pom.xml,depends,src} $(TMP_DIR)/client
	cp -r /tmp/doc $(TMP_DIR)/client
	cp -r examples/{README,pom.xml,run_examples,run_examples_swing,src,udf} $(TMP_DIR)/examples
	cp -r benchmarks/{README,pom.xml,run_benchmarks,src} $(TMP_DIR)/benchmarks
	cp -r servlets/{README,pom.xml,src,webapp} $(TMP_DIR)/servlets
	tar cf - -C /tmp $(NAME) | gzip -c > package/$(NAME).tgz
	sh -c 'cd /tmp ; zip -r $(NAME).zip $(NAME)'
	mv /tmp/$(NAME).zip package
	cp client/target/aerospike-client-* package	
	rm -rf $(TMP_DIR)
