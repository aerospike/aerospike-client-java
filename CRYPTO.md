GNU Crypto vs Bouncy Castle
---------------------------

A RipeMD-160 hash is performed on every key sent to an Aerospike Server.
AerospikeClient uses GNU Crypto's RipeMD-160 hash algorithm by default due to 
its superior performance over Bouncy Castle's algorithm.  The overall
AerospikeClient performance difference is much less noticeable because the hash
is only one of many factors that determine performance.

GNU Crypto uses a GPL based license with a "library exception" which permits its
use as a library in conjunction with non-Free software
(See [GNU Crypto License](http://www.gnu.org/software/gnu-crypto)).
Even with this explicit exception, some organizations still choose to ban applications
that link with the GNU Crypto library
(See [APACHE](https://www.apache.org/legal/resolved.html#category-x)).

AerospikeClient can now be configured to use either GNU Crypto or Bouncy Castle.
Bouncy Castle uses a MIT based license.

Usage:

    ./set_crypto gnu | bouncycastle

Examples:

    ./set_crypto gnu
    ./set_crypto bouncycastle
