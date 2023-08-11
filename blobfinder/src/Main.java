import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.BlobFinder;

/**
 * Example code to find language specific blobs in an Aerospike database.
 * Must be used with aerospike-blob-finder library.
 * <p>
 * Edit this code to make adjustments to ClientPolicy for login and to 
 * specify different run arguments.
 */
public class Main {

	public static void main(String[] args) {
		try {
			if (args.length == 0) {
				System.out.println("Usage: " + Main.class.getName() + " hostname1[:tlsname1][:port1],...");
				return;
			}

			Host[] hosts = Host.parseHosts(args[0], 3000);

			ClientPolicy policy = new ClientPolicy();

			AerospikeClient client = new AerospikeClient(policy, hosts);

			try {
				BlobFinder.run(client, "blobs.dat", 100000);
			}
			finally {
				client.close();
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
	}
}
