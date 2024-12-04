#!/bin/bash -e
# The default crypto library is gnu-crypto.
# This script can change crypto library to bouncy castle's bcprov-jdk15on.

case "$1" in
gnu)
  set -x
  cp client/crypto/gnu/Crypto.java client/src/com/aerospike/client/util
  sed 's/org.bouncycastle/org.gnu/;s/bcprov-jdk15on/gnu-crypto/;s/1.69/2.0.1/' client/pom.xml >tmp.xml
  mv tmp.xml client/pom.xml
  ;;

bouncycastle)
  set -x
  cp client/crypto/bouncycastle/Crypto.java client/src/com/aerospike/client/util
  sed 's/org.gnu/org.bouncycastle/;s/gnu-crypto/bcprov-jdk15on/;s/2.0.1/1.69/' client/pom.xml >tmp.xml
  mv tmp.xml client/pom.xml
  ;;

*)
  echo "Usage: $0 gnu | bouncycastle"
  # Anything other than 0 is considered an error of some kind. Values outside 0 - 255 should be written to stderr or stdout.
  exit 1
  ;;
esac
