# Package Java Client

SHELL=bash
REV=$(shell scripts/version)
NAME=aerospike-client-java-$(REV)
TMP_DIR=/tmp/$(NAME)

.PHONY: default
default:
	rm -rf package/aerospike-client-java*
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	mkdir -p $(TMP_DIR)/client
	mkdir -p $(TMP_DIR)/examples
	mkdir -p $(TMP_DIR)/benchmarks
	mkdir -p $(TMP_DIR)/servlets
	cp package/{README,build_all,license.txt} $(TMP_DIR)
	cp -r client/{README,depends,src} $(TMP_DIR)/client
	cp -r /tmp/doc $(TMP_DIR)/client
	cp -r examples/{README,run_examples,run_examples_swing,src,udf} $(TMP_DIR)/examples
	cp -r benchmarks/{README,run_benchmarks,src} $(TMP_DIR)/benchmarks
	cp -r servlets/{README,src,webapp} $(TMP_DIR)/servlets
	scripts/package_pom pom.xml > $(TMP_DIR)/pom.xml
	scripts/package_pom client/pom.xml > $(TMP_DIR)/client/pom.xml
	scripts/package_pom examples/pom.xml > $(TMP_DIR)/examples/pom.xml
	scripts/package_pom benchmarks/pom.xml > $(TMP_DIR)/benchmarks/pom.xml
	scripts/package_pom servlets/pom.xml > $(TMP_DIR)/servlets/pom.xml
	tar cf - -C /tmp $(NAME) | gzip -c > package/$(NAME).tgz
	rm -rf $(TMP_DIR)
