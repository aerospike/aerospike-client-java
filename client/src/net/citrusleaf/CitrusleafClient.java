/*
 * Citrusleaf Aerospike Client - Java Library
 *
 * Copyright 2009-2010 by Citrusleaf, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package net.citrusleaf;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Value;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Legacy compatibility Layer. This class should only be used for legacy code.
 * Please use <code>com.aerospike.client.AerospikeClient</code> for new code.
 * <p>
 * Instantiate a <code>CitrusleafClient</code> object to access a Citrusleaf
 * database cluster and perform database operations.
 * <p>
 * Your application uses this class API to perform database operations such as
 * writing and reading records, and scanning sets of records. Write operations
 * include specialized functionality such as append/prepend and arithmetic
 * addition.
 * <p>
 * Records are stored and identified using a specified namespace, an optional
 * set name, and a key which must be unique within a set.
 * <p>
 * Each record may have multiple bins, unless the Citrusleaf server nodes are
 * configured as "single-bin". In "multi-bin" mode, partial records may be
 * written or read by specifying the relevant subset of bins.
 */
public class CitrusleafClient extends AerospikeClient implements Log.Callback {

	//=================================================================
	// Constants
	//

	/**
	 * Database operation result codes.
	 */
	public enum ClResultCode {
		/**
		 * Operation was successful.
		 */
		OK,

		/**
		 * Initial "empty" value, operation has not started.
		 */
		NOT_SET,

		/**
		 * Unknown server failure.
		 */
		SERVER_ERROR,

		/**
		 * Operation timed out.
		 */
		TIMEOUT,

		/**
		 * Memory or other client fault.
		 */
		CLIENT_ERROR,

		/**
		 * On retrieving, touching or replacing a record that doesn't exist.
		 */
		KEY_NOT_FOUND_ERROR,

		/**
		 * On modifying a record with unexpected generation.
		 */
		GENERATION_ERROR,

		/**
		 * Bad parameter(s) were passed in database operation call.
		 */
		PARAMETER_ERROR,

		/**
		 * On create-only (write unique) operations on a record that already
		 * exists.
		 */
		KEY_EXISTS_ERROR,

		/**
		 * On create-only (write unique) operations on a bin that already
		 * exists.
		 */
		BIN_EXISTS_ERROR,

		/**
		 * Object could not be serialized.
		 */
		SERIALIZE_ERROR,

		/**
		 * Expected cluster ID was not received.
		 */
		CLUSTER_KEY_MISMATCH,

		/**
		 * Server has run out of memory.
		 */
		SERVER_MEM_ERROR,

		/**
		 * XDS product is not available.
		 */
		NO_XDS,

		/**
		 * Server is not accepting requests.
		 */
		SERVER_NOT_AVAILABLE,

		/**
		 * Operation is not supported with configured bin type (single-bin or
		 * multi-bin).
		 */
		BIN_TYPE_ERROR,

		/**
		 * Record size exceeds limit.
		 */
		RECORD_TOO_BIG,

		/**
		 * Too many concurrent operations on the same record.
		 */
		KEY_BUSY
	}

	/**
	 * Priority of scan operations on database server.
	 */
	public enum ClScanningPriority {
		/**
		 * A server node autonomously decides how many scan threads to use based
		 * on factors such as the number of storage devices.
		 */
		AUTO,

		/**
		 * Currently this means a server node will use one scan thread.
		 */
		LOW,

		/**
		 * Currently this means a server node will use three scan threads.
		 */
		MEDIUM,

		/**
		 * Currently this means a server node will use five scan threads.
		 */
		HIGH
	}

	/**
	 * Log escalation level.
	 */
	public enum ClLogLevel {
		/**
		 * Error condition has occurred.
		 */
		ERROR,

		/**
		 * Unusual non-error condition has occurred.
		 */
		WARN,

		/**
		 * Normal information message.
		 */
		INFO,

		/**
		 * Message used for debugging purposes.
		 */
		DEBUG,

		/**
		 * Detailed message also used for debugging purposes.
		 */
		VERBOSE
	}

	// Package-Private
	static final int DEFAULT_TIMEOUT = 5000;

	private enum RetryPolicy {
		ONE_SHOT,
		RETRY
	}

	private static final String DEFAULT_NAMESPACE = "ns";


	//=================================================================
	// Globals
	//

	private static SimpleDateFormat gSdf = null;
	private static ClLogLevel gLogLevel = ClLogLevel.INFO;
	private static LogCallback gLogCallback = null;

	//=================================================================
	// Member Data
	//

	private String mDefaultNamespace = DEFAULT_NAMESPACE;
	private final WritePolicy defaultWritePolicy;
	private final Policy defaultPolicy;


	//=================================================================
	// Public Functions
	//

	//-------------------------------------------------------
	// Constructors
	//-------------------------------------------------------

	/**
	 * Constructor, creates <code>CitrusleafClient</code> object and initializes
	 * logging framework.
	 * <p>
	 * This constructor does not add hosts.
	 */
	public CitrusleafClient() {
		ClLogInit();
		defaultWritePolicy = new WritePolicy();
		defaultWritePolicy.timeout = DEFAULT_TIMEOUT;
		defaultPolicy = new Policy();
		defaultPolicy.timeout = DEFAULT_TIMEOUT;
	}

	/**
	 * Constructor, combines default constructor {@link #CitrusleafClient()} and
	 * {@link #addHost(String, int) addHost()}.
	 * 
	 * @param hostname			host name
	 * @param port				host port
	 */
	public CitrusleafClient(String hostname, int port) {
		ClLogInit();
		defaultWritePolicy = new WritePolicy();
		defaultWritePolicy.timeout = DEFAULT_TIMEOUT;
		defaultPolicy = new Policy();
		defaultPolicy.timeout = DEFAULT_TIMEOUT;
		
		try {
			super.addServer(hostname, port);
		}
		catch (Exception e) {
		}
	}

	//-------------------------------------------------------
	// Do Logging via Callback
	//-------------------------------------------------------

	/**
	 * Set logging level filter and optional log callback implementation.
	 * <p>
	 * This method may only be called once, at startup.
	 * 
	 * @param level				only show logs at this or more urgent level
	 * @param logCb				{@link LogCallback} implementation, pass
	 * 							<code>null</code> to let Citrusleaf client write
	 * 							logs to <code>System.err</code>
	 */
	public static void setLogging(ClLogLevel level, LogCallback logCb) {
		gLogLevel = level;
		gLogCallback = logCb;
		
		Log.Level ll;
		
		switch (level) {
		case ERROR:
			ll = Log.Level.ERROR;
			break;
		case WARN:
			ll = Log.Level.WARN;
			break;
		case INFO:
		default:
			ll = Log.Level.INFO;
			break;
		case DEBUG:
		case VERBOSE:
			ll = Log.Level.DEBUG;
			break;
		}
		Log.setLevel(ll);
	}

	private static final ClLogLevel[] ClLogLevelLookup = ClLogLevel.values();

	/**
	 * Implementation of com.aerospike.client.Log.Callback
	 */
	@Override
	public void log(Level level, String message) {
		ClLogLevel cll = ClLogLevelLookup[level.ordinal()];		
		ClLog(cll, message);
	}

	//-------------------------------------------------------
	// Cluster Connection Management
	//-------------------------------------------------------

	/**
	 * Add a server database host to the client's cluster map.
	 * <p>
	 * The following actions occur upon the first invocation of this method:
	 * <p>
	 * - create new cluster map <br>
	 * - add host to cluster map <br>
	 * - connect to host server <br>
	 * - request host's list of other nodes in cluster <br>
	 * - add these nodes to cluster map <br>
	 * <p>
	 * Further invocations will add hosts to the cluster map if they don't
	 * already exist. In most cases, only one <code>addHost()</code> call is
	 * necessary. When this call succeeds, the client is ready to process
	 * database requests.
	 * 
	 * @param hostname			host name
	 * @param port				host port
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode addHost(String hostname, int port) {
		try {
			super.addServer(hostname, port);
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}
	
	/**
	 * Use {@link #isConnected()}.
	 */
	@Deprecated
	public boolean connect() {
		return super.isConnected();
	}

	/**
	 * Return a list of server nodes in the cluster.
	 * 
	 * @return					list of node names
	 */
	public List<String> getNodeNameList() {
		return super.getNodeNames();
	}

	/**
	 * Specify namespace to use in database operation calls that do not have a
	 * namespace parameter.
	 * 
	 * @param namespace			namespace to use in calls that have no namepsace
	 * 							parameter
	 */
	public void setDefaultNamespace(String namespace) {
		mDefaultNamespace = namespace;
	}

	//-------------------------------------------------------
	// Key Digest Computation
	//-------------------------------------------------------

	/**
	 * Generate digest from key and set name.
	 * 
	 * @param set				set name
	 * @param key				record identifier, unique within set
	 * @return					unique identifier generated from key and set
	 * 							name
	 */
	public static byte[] computeDigest(String set, Object key) {
		try {
			return Key.computeDigest(set, Value.get(key));
		}
		catch (AerospikeException ae) {
			return null;
		}
	}

	//-------------------------------------------------------
	// Set Operations
	//-------------------------------------------------------

	/**
	 * For server nodes configured as "single-bin" only - set record value for
	 * specified key, using default namespace, no set name, and default options.
	 * <p>
	 * If the record does not exist, it will be created. If it exists its value
	 * is replaced.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				single-bin value
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode set(Object key, Object value) {
		try {
			super.put(defaultWritePolicy, new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For server nodes configured as "single-bin" only - set record value for
	 * specified key, using default namespace and no set name.
	 * <p>
	 * If the record does not exist, it will be created. If it exists its value
	 * is replaced.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				single-bin value
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode set(Object key, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Set record bin value for specified key and bin name.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binName			bin name, pass <code>""</code> if server nodes
	 * 							are configured as "single-bin"
	 * @param value				bin value
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode set(String namespace, String set, Object key,
			String binName, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, set, key), new Bin(binName, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Set record bin values for specified key and bin names.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode set(String namespace, String set, Object key,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}	
	
	/**
	 * Set record bin values for specified key and bin names.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode set(String namespace, String set, Object key,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	//-------------------------------------------------------
	// Set Operations - by digest
	//-------------------------------------------------------

	/**
	 * Set record bin value for specified key digest and bin name.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param binName			bin name, pass <code>""</code> if server nodes
	 * 							are configured as "single-bin"
	 * @param value				bin value
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode setDigest(String namespace, byte[] digest,
			String binName, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, digest), new Bin(binName, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Set record bin value for specified key digest and bin name.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param bin				bin name/value pair
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode setDigest(String namespace, byte[] digest, ClBin bin,
			ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, digest), new Bin(bin.name, bin.value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Set record bin values for specified key digest and bin names.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode setDigest(String namespace, byte[] digest,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, digest), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Set record bin values for specified key digest and bin names.
	 * <p>
	 * If the record or bin does not exist, it will be created. If the bin
	 * exists its value is replaced. Others bins are ignored.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode setDigest(String namespace, byte[] digest,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.put(getWritePolicy(opts, wOpts), new Key(namespace, digest), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	//-------------------------------------------------------
	// Append & Prepend Operations
	//-------------------------------------------------------

	/**
	 * For server nodes configured as "single-bin" only - append value to
	 * existing record value for specified key, using default namespace, no set
	 * name, and default options.
	 * <p>
	 * If the record does not exist, it will be created with the specified
	 * value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				value to append
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode append(Object key, Object value) {
		try {
			super.append(defaultWritePolicy, new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For server nodes configured as "single-bin" only - append value to
	 * existing record value for specified key, using default namespace and no
	 * set name.
	 * <p>
	 * If the record does not exist, it will be created with the specified
	 * value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				value to append
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode append(Object key, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.append(getWritePolicy(opts, wOpts), new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Append value to existing bin value for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binName			bin name, pass <code>""</code> if server nodes
	 * 							are configured as "single-bin"
	 * @param value				value to append
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode append(String namespace, String set, Object key,
			String binName, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.append(getWritePolicy(opts, wOpts), new Key(namespace, set, key), new Bin(binName, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Append value to existing bin value for each bin for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode append(String namespace, String set, Object key,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.append(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Append value to existing bin value for each bin for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode append(String namespace, String set, Object key,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.append(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For server nodes configured as "single-bin" only - prepend value to
	 * existing record value for specified key, using default namespace, no set
	 * name, and default options.
	 * <p>
	 * If the record does not exist, it will be created with the specified
	 * value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				value to prepend
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode prepend(Object key, Object value) {
		try {
			super.prepend(defaultWritePolicy, new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For server nodes configured as "single-bin" only - prepend value to
	 * existing record value for specified key, using default namespace and no
	 * set name.
	 * <p>
	 * If the record does not exist, it will be created with the specified
	 * value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				value to prepend
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode prepend(Object key, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.prepend(getWritePolicy(opts, wOpts), new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Prepend value to existing bin value for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binName			bin name, pass <code>""</code> if server nodes
	 * 							are configured as "single-bin"
	 * @param value				value to prepend
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode prepend(String namespace, String set, Object key,
			String binName, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.prepend(getWritePolicy(opts, wOpts), new Key(namespace, set, key), new Bin(binName, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Prepend value to existing bin value for each bin for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode prepend(String namespace, String set, Object key,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.prepend(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Prepend value to existing bin value for each bin for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * This call works only if the specified value's type matches the existing
	 * value's type, and the type is not an integer (i.e. <code>Integer</code>
	 * or <code>Long</code>.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode prepend( String namespace, String set, Object key,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.prepend(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------

	/**
	 * For integer values, and server nodes configured as "single-bin" only -
	 * add value to existing record value for specified key, using default
	 * namespace, no set name, and default options.
	 * <p>
	 * If the record does not exist, it will be created with the specified
	 * value.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				bin value
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode add(Object key, Object value) {
		try {
			super.add(defaultWritePolicy, new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values, and server nodes configured as "single-bin" only -
	 * add value to existing record value for specified key, using default
	 * namespace and no set name.
	 * <p>
	 * If the record does not exist, it will be created with the specified
	 * value.
	 * 
	 * @param key				record identifier, unique within set
	 * @param value				bin value
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode add(Object key, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(mDefaultNamespace, "", key), new Bin(null, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for specified
	 * key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binName			bin name, pass <code>""</code> if server nodes
	 * 							are configured as "single-bin"
	 * @param value				bin value
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode add(String namespace, String set, Object key,
			String binName, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, set, key), new Bin(binName, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for each bin
	 * for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode add(String namespace, String set, Object key,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for each bin
	 * for specified key.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode add(String namespace, String set, Object key,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, set, key), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for each bin
	 * for specified key, and return the results.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 * 							with new values
	 */
	public ClResult addAndGet(String namespace, String set, Object key,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		Operation[] operations = new Operation[bins.size() + 1];
		int count = 0;
		
		for (ClBin bin : bins) {
			operations[count++] = Operation.add(new Bin(bin.name, bin.value));
		}
		operations[count] = Operation.get();

		try {
			Record record = super.operate(getWritePolicy(opts, wOpts), new Key(namespace, set, key), operations);
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for each bin
	 * for specified key, and return the results.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 * 							with new values
	 */
	public ClResult addAndGet(String namespace, String set, Object key,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		Operation[] operations = new Operation[bins.size() + 1];
		int count = 0;
		
		for (Entry<String,Object> entry : bins.entrySet()) {
			operations[count++] = Operation.add(new Bin(entry.getKey(), entry.getValue()));
		}
		operations[count] = Operation.get();

		try {
			Record record = super.operate(getWritePolicy(opts, wOpts), new Key(namespace, set, key), operations);
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	//-------------------------------------------------------
	// Arithmetic Operations - by digest
	//-------------------------------------------------------

	/**
	 * For integer values only - add value to existing bin value for specified
	 * key digest.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param binName			bin name, pass <code>""</code> if server nodes
	 * 							are configured as "single-bin"
	 * @param value				bin value
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode addDigest(String namespace, byte[] digest,
			String binName, Object value, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, digest), new Bin(binName, value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for specified
	 * key digest.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param bin				bin name/value pair
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode addDigest(String namespace, byte[] digest, ClBin bin,
			ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, digest), new Bin(bin.name, bin.value));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for each bin
	 * for specified key digest.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param bins				bin name/value pairs as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode addDigest(String namespace, byte[] digest,
			Collection<ClBin> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, digest), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * For integer values only - add value to existing bin value for each bin
	 * for specified key digest.
	 * <p>
	 * If the record or bin does not exist, it will be created with the
	 * specified value.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param bins				bin name/value pairs as <code>Map</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode addDigest(String namespace, byte[] digest,
			Map<String, Object> bins, ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.add(getWritePolicy(opts, wOpts), new Key(namespace, digest), getBins(bins));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	/**
	 * Delete record for specified key, using default namespace and no set name.
	 * 
	 * @param key				record identifier, unique within set
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode delete(Object key, ClOptions opts,
			ClWriteOptions wOpts) {
		try {
			if (super.delete(getWritePolicy(opts, wOpts), new Key(mDefaultNamespace, "", key))) {
				return ClResultCode.OK;				
			}
			else {
				return ClResultCode.KEY_NOT_FOUND_ERROR;				
			}
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Delete record for specified key.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode delete(String namespace, String set, Object key,
			ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.delete(getWritePolicy(opts, wOpts), new Key(namespace, set, key));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	//-------------------------------------------------------
	// Delete Operations - by digest
	//-------------------------------------------------------

	/**
	 * Delete record for specified key digest.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode deleteDigest(String namespace, byte[] digest,
			ClOptions opts, ClWriteOptions wOpts) {
		try {
			super.delete(getWritePolicy(opts, wOpts), new Key(namespace, digest));
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	//-------------------------------------------------------
	// Get Operations
	//-------------------------------------------------------

	/**
	 * For server nodes configured as "single-bin" only - get record value for
	 * specified key, using default namespace, no set name, and default options.
	 * 
	 * @param key				record identifier, unique within set
	 * @return					{@link ClResult} containing single-bin value
	 */
	public Object get(Object key) {
		try {
			Record record = super.get(defaultPolicy, new Key(mDefaultNamespace, "", key), "");
			ClResult r = getResult(record);
			// Single-bin result is in results Map, also put it in result Object:
			return r.results != null ? r.results.get("") : null;
		}
		catch (AerospikeException ae) {
			return null;
		}
	}

	/**
	 * Get bin value for specified key and bin name.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binName			bin name filter, pass <code>""</code> if server
	 * 							nodes are configured as "single-bin"
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin value
	 */
	public ClResult get(String namespace, String set, Object key,
			String binName, ClOptions opts) {
		try {
			String name = (binName == null)? "" : binName;
			Record record = super.get(getPolicy(opts), new Key(namespace, set, key), name);
			ClResult r = getResult(record);
			// Result for one bin is in results Map, also put it in result Object:
			if (r.results != null) {
				r.result = r.results.get(name);
			}
			return r;
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Get bin name/value pairs for specified key and list of bin names.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binNames			bin names filter as array
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 */
	public ClResult get(String namespace, String set, Object key,
			String[] binNames, ClOptions opts) {
		try {
			Record record = super.get(getPolicy(opts), new Key(namespace, set, key), binNames);
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Get bin name/value pairs for specified key and list of bin names.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binNames			bin names filter as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 */
	public ClResult get(String namespace, String set, Object key,
			Collection<String> binNames, ClOptions opts) {
		try {
			Record record = super.get(getPolicy(opts), new Key(namespace, set, key), getBinNames(binNames));
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Get all bin name/value pairs for specified key.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 */
	public ClResult getAll(String namespace, String set, Object key,
			ClOptions opts) {
		try {
			Record record = super.get(getPolicy(opts), new Key(namespace, set, key));
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}	

	/**
	 * Get bin name/value pairs for specified key and list of bin names, and if
	 * the record exists, reset record time to expiration.
	 * <p>
	 * This operation will increment the record generation.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param binNames			bin names filter as <code>Collection</code>
	 * @param expiration		new record expiration (see
	 * 							{@link ClWriteOptions#expiration})
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 */
	public ClResult getWithTouch(String namespace, String set, Object key,
			Collection<String> binNames, int expiration, ClOptions opts) {
		ClWriteOptions wOpts = new ClWriteOptions();
		wOpts.expiration = expiration;
		
		Operation[] operations = new Operation[binNames.size() + 1];
		int count = 0;
		
		for (String binName : binNames) {
			operations[count++] = Operation.get(binName);
		}
		operations[count] = Operation.touch();

		try {
			Record record = super.operate(getWritePolicy(opts, wOpts), new Key(namespace, set, key), operations);
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Batch multiple <code>get()</code> requests into a single call.
	 * <p>
	 * Elements of <code>keys</code> are positionally matched with elements of
	 * result array.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param keys				batch of keys as <code>Collection</code>
	 * @param binName			bin name filter, pass <code>""</code> if server
	 * 							nodes are configured as "single-bin"
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult result} array where each element
	 * 							contains bin name/value pairs
	 */
	public ClResult[] batchGet(String namespace, String set,
			Collection<Object> keys, String binName, ClOptions opts) {
		try {
			Key[] keyArray = getKeys(namespace, set, keys);
			Record[] records = super.get(getPolicy(opts), keyArray, binName);
			return getRecordResults(records);
		}
		catch (AerospikeException ae) {
			return getResults(ae, keys.size());
		}
	}

	/**
	 * Batch multiple <code>get()</code> requests into a single call.
	 * <p>
	 * Elements of <code>keys</code> are positionally matched with elements of
	 * result array.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param keys				batch of keys as <code>Collection</code>
	 * @param binNames			bin names filter as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult result} array where each element
	 * 							contains bin name/value pairs
	 */
	public ClResult[] batchGet(String namespace, String set,
			Collection<Object> keys, Collection<String> binNames,
			ClOptions opts) {
		try {
			Key[] keyArray = getKeys(namespace, set, keys);
			Record[] records = super.get(getPolicy(opts), keyArray, getBinNames(binNames));
			return getRecordResults(records);
		}
		catch (AerospikeException ae) {
			return getResults(ae, keys.size());
		}
	}

	/**
	 * Batch multiple <code>getAll()</code> requests into a single call.
	 * <p>
	 * Elements of <code>keys</code> are positionally matched with elements of
	 * result array.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param keys				batch of keys as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult result} array where each element
	 * 							contains bin name/value pairs
	 */
	public ClResult[] batchGetAll(String namespace, String set,
			Collection<Object> keys, ClOptions opts) {
		try {
			Key[] keyArray = getKeys(namespace, set, keys);
			Record[] records = super.get(getPolicy(opts), keyArray);
			return getRecordResults(records);
		}
		catch (AerospikeException ae) {
			return getResults(ae, keys.size());
		}
	}

	//-------------------------------------------------------
	// Get Operations - by digest
	//-------------------------------------------------------

	/**
	 * Get bin value for specified key digest and bin name.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param binName			bin name filter, pass <code>""</code> if server
	 * 							nodes are configured as "single-bin"
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin value
	 */
	public ClResult getDigest(String namespace, byte[] digest, String binName,
			ClOptions opts) {
		try {
			Record record = super.get(getPolicy(opts), new Key(namespace, digest), binName);
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Get bin name/value pairs for specified key digest and list of bin names.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param binNames			bin names filter as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 */
	public ClResult getDigest(String namespace, byte[] digest,
			Collection<String> binNames, ClOptions opts) {
		try {
			Record record = super.get(getPolicy(opts), new Key(namespace, digest), getBinNames(binNames));
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Get all bin name/value pairs for specified key digest.
	 * <p>
	 * Non-Digest database methods send the normal key and set name to the
	 * server which converts them to a digest. Digest methods send the digest
	 * directly.
	 * 
	 * @param namespace			namespace
	 * @param digest			unique identifier generated from key and set
	 * 							name
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} containing bin name/value pairs
	 */
	public ClResult getAllDigest(String namespace, byte[] digest,
			ClOptions opts) {
		try {
			Record record = super.get(getPolicy(opts), new Key(namespace, digest));
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	/**
	 * Check if specified key exists, using default namespace, no set name, and
	 * default options.
	 * 
	 * @param key				record identifier, unique within set
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode exists(Object key) {
		try {
			boolean exists = super.exists(defaultPolicy, new Key(mDefaultNamespace, "", key));
			return exists? ClResultCode.OK : ClResultCode.KEY_NOT_FOUND_ERROR;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Check if specified key exists.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode exists(String namespace, String set, Object key,
			ClOptions opts) {
		try {
			boolean exists = super.exists(getPolicy(opts), new Key(namespace, set, key));
			return exists? ClResultCode.OK : ClResultCode.KEY_NOT_FOUND_ERROR;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Batch multiple <code>exists()</code> requests into a single call.
	 * <p>
	 * Elements of <code>keys</code> are positionally matched with elements of
	 * result array.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param keys				batch of keys as <code>Collection</code>
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult result} array where each element
	 * 							indicates whether its corresponding key exists
	 */
	public ClResult[] batchExists(String namespace, String set,
			Collection<Object> keys, ClOptions opts) {
		try {
			Key[] keyArray = getKeys(namespace, set, keys);
			boolean[] existsArray = super.exists(getPolicy(opts), keyArray);
			return getKeyResults(existsArray);
		}
		catch (AerospikeException ae) {
			return getResults(ae, keys.size());
		}
	}

	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------

	/**
	 * Scan the database, retrieving all records in specified namespace.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResult} for final record
	 */
	@Deprecated
	public ClResult scan(String namespace, ClOptions opts, ScanCallback scanCb,
			Object userData) {
		return scan(namespace, null, opts, scanCb, userData, false);
	}

	/**
	 * Scan the database, retrieving all records in specified namespace.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param set				set name, not supported as scan filter
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResult} for final record
	 */
	@Deprecated
	public ClResult scan(String namespace, String set, ClOptions opts,
			ScanCallback scanCb, Object userData) {
		return scan(namespace, set, opts, scanCb, userData, false);
	}

	/**
	 * Scan the database using specified namespace filter, retrieving records or
	 * digests only, as specified.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResult} for final record
	 */
	@Deprecated
	public ClResult scan(String namespace, ClOptions opts, ScanCallback scanCb,
			Object userData, boolean noBinData) {
		return scan(namespace, null, opts, scanCb, userData, noBinData);
	}

	/**
	 * Scan the database using specified namespace filter, retrieving records or
	 * digests only, as specified.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param set				set name, not supported as scan filter
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @param noBinData			<code>true</code> to retrieve digests only,
	 * 							<code>false</code> to retrieve bin data
	 * @return					{@link ClResult} for final record
	 */
	@Deprecated
	public ClResult scan(String namespace, String set, ClOptions opts,
			ScanCallback scanCb, Object userData, boolean noBinData) {
		try {		
			ScanForwarder sf = new ScanForwarder(scanCb, userData);
			super.scanAll(getScanPolicy(opts, null, noBinData), namespace, set, sf);
			return new ClResult(ClResultCode.OK);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	/**
	 * Scan a single database node, using specified namespace and set filters,
	 * retrieving records or digests only, as specified.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param nodeName			node to scan
	 * @param namespace			namespace
	 * @param set				set name, pass <code>null</code> to retrieve all
	 * 							records in the namespace
	 * @param noBinData			<code>true</code> to retrieve digests only,
	 * 							<code>false</code> to retrieve bin data
	 * @param scanPercent		fraction of data to scan - not yet supported
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanOpts			parallel scan policy attributes, pass
	 * 							<code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResultCode result status}
	 */
	public ClResultCode scanNode(String nodeName, String namespace, String set,
			boolean noBinData, int scanPercent, ClOptions opts,
			ClScanningOptions scanOpts, ScanCallback scanCb, Object userData) {		
		try {		
			ScanForwarder sf = new ScanForwarder(scanCb, userData);
			super.scanNode(getScanPolicy(opts, scanOpts, noBinData), nodeName, namespace, set, sf);
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Scan the database nodes in parallel, using specified namespace and set
	 * filters, and default options, returning a single result code.
	 * <p>
	 * This call is the same as
	 * {@link #scanAllNodes(String, String, ScanCallback, Object)
	 * scanAllNodes()} but returns only one result code.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param set				set name, pass <code>null</code> to retrieve all
	 * 							records in the namespace
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResultCode result status},
	 * 							{@link ClResultCode#OK OK} if all nodes
	 * 							succeeded, else first error code encountered
	 */
	public ClResultCode scan(String namespace, String set, ScanCallback scanCb,
			Object userData) {
		try {		
			ScanForwarder sf = new ScanForwarder(scanCb, userData);
			super.scanAll(getScanPolicy(null, null, false), namespace, set, sf);
			return ClResultCode.OK;
		}
		catch (AerospikeException ae) {
			return getResultCode(ae);
		}
	}

	/**
	 * Scan the database nodes in parallel, using specified namespace and set
	 * filters, and default options.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param set				set name, pass <code>null</code> to retrieve all
	 * 							records in the namespace
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResultCode result status} for each node
	 */
	public Map<String, ClResultCode> scanAllNodes(String namespace, String set,
			ScanCallback scanCb, Object userData) {
		return scanAllNodes(namespace, set, false, 100, null, null, scanCb,
				userData);
	}

	/**
	 * Scan the database nodes in parallel, using specified namespace and set
	 * filters.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param set				set name, pass <code>null</code> to retrieve all
	 * 							records in the namespace
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanOpts			parallel scan policy attributes, pass
	 * 							<code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResultCode result status} for each node
	 */
	public Map<String, ClResultCode> scanAllNodes(String namespace, String set,
			ClOptions opts, ClScanningOptions scanOpts, ScanCallback scanCb,
			Object userData) {
		return scanAllNodes(namespace, set, false, 100, opts, scanOpts, scanCb,
				userData);		
	}

	/**
	 * Scan the database nodes in parallel, using specified namespace and set
	 * filters, retrieving records or digests only, as specified.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param namespace			namespace
	 * @param set				set name, pass <code>null</code> to retrieve all
	 * 							records in the namespace
	 * @param noBinData			<code>true</code> to retrieve digests only,
	 * 							<code>false</code> to retrieve bin data
	 * @param scanPercent		fraction of data to scan - not yet supported
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param scanOpts			parallel scan policy attributes, pass
	 * 							<code>null</code> to use defaults
	 * @param scanCb			{@link ScanCallback} implementation
	 * @param userData			pass-through user-defined data
	 * @return					{@link ClResultCode result status} for each node
	 */
	public Map<String, ClResultCode> scanAllNodes(String namespace, String set,
			boolean noBinData, int scanPercent, ClOptions opts,
			ClScanningOptions scanOpts, ScanCallback scanCb, Object userData) {
		List<String> nodeNames = super.getNodeNames();
		
		try {		
			ScanForwarder sf = new ScanForwarder(scanCb, userData);
			super.scanAll(getScanPolicy(opts, scanOpts, noBinData), namespace, set, sf);
			return getResultCodes(ClResultCode.OK, nodeNames);
		}
		catch (AerospikeException ae) {
			return getResultCodes(ae, nodeNames);
		}
	}

	//-------------------------------------------------------
	// Low-level Operations
	//-------------------------------------------------------

	/**
	 * Combine <code>get()</code> and <code>set()</code> operations for a
	 * specified record.
	 * 
	 * @param namespace			namespace
	 * @param set				optional set name
	 * @param key				record identifier, unique within set
	 * @param readBinNames		bin names filter for read
	 * @param writeBins			bin name/value pairs to write
	 * @param opts				{@link ClOptions transaction policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @param wOpts				{@link ClWriteOptions write policy attributes},
	 * 							pass <code>null</code> to use defaults
	 * @return					{@link ClResult} for read, containing bin
	 * 							name/value pairs
	 */
	public ClResult operate(String namespace, String set, Object key,
			Collection<String> readBinNames, Collection<ClBin> writeBins,
			ClOptions opts, ClWriteOptions wOpts) {
		Operation[] operations = new Operation[readBinNames.size() + writeBins.size()];
		int count = 0;
		
		for (ClBin bin : writeBins) {
			operations[count++] = Operation.put(new Bin(bin.name, bin.value));
		}

		for (String binName : readBinNames) {
			operations[count++] = Operation.get(binName);
		}
		
		try {
			Record record = super.operate(getWritePolicy(opts, wOpts), new Key(namespace, set, key), operations);
			return getResult(record);
		}
		catch (AerospikeException ae) {
			return getResult(ae);
		}
	}

	//=================================================================
	// Public Nested Interfaces & Classes
	//

	//-------------------------------------------------------
	// Callbacks
	//-------------------------------------------------------

	/**
	 * An object implementing this interface may be passed in
	 * <code>setLogging()</code>, so the caller can channel Citrusleaf client
	 * logs as desired.
	 */
	public static interface LogCallback {
		/**
		 * This method will be called for each Citrusleaf client log statement.
		 * 
		 * @param level				{@link ClLogLevel log level}
		 * @param msg				log message
		 */
		public void logCallback(ClLogLevel level, String msg);
	}

	/**
	 * An object implementing this interface is passed in <code>scan...()</code>
	 * calls, so the caller can be notified with scan results.
	 */
	public static interface ScanCallback {
		/**
		 * This method will be called for each record returned from a scan.
		 * 
		 * @param namespace			namespace
		 * @param set				set name
		 * @param digest			unique ID generated from key and set name
		 * @param bins				bin name/value pairs as <code>Map</code>
		 * @param generation		how many times the record has been modified
		 * @param expirationDate	date this record will expire, in seconds
		 * 							from Jan 01 2010 00:00:00 GMT
		 * @param userData			pass-through user-defined data
		 */
		public void scanCallback(String namespace, String set, byte[] digest,
				Map<String, Object> bins, int generation, int expirationDate,
				Object userData);
	}

	//-------------------------------------------------------
	// Transaction Options
	//-------------------------------------------------------

	/**
	 * Container object for transaction policy attributes used in all database
	 * operation calls.
	 * <p>
	 * This object is passed in all database operation calls to specify options.
	 * <code>null</code> may be passed to use defaults for all options.
	 */
	public static class ClOptions {
		// Package-Private
		int mTimeout;
		RetryPolicy mRetryPolicy;

		/**
		 * Constructor, sets default options.
		 * <p>
		 * The object is constructed with timeout set to <code>5000</code>
		 * milliseconds and default transaction retry policy.
		 */
		public ClOptions() {
			mTimeout = DEFAULT_TIMEOUT;
			mRetryPolicy = RetryPolicy.RETRY;
		}

		/**
		 * Constructor, specifying timeout.
		 * <p>
		 * The object is constructed with specified timeout and default
		 * transaction retry policy.
		 * <p>
		 * A timeout value of <code>0</code> means never timeout.
		 * 
		 * @param timeoutMillisec	transaction duration limit, milliseconds
		 */
		public ClOptions(int timeoutMillisec) {
			mTimeout = timeoutMillisec;
			mRetryPolicy = RetryPolicy.RETRY;
		}

		/**
		 * Specify no transaction retries.
		 */
		public void setOneShot() {
			mRetryPolicy = RetryPolicy.ONE_SHOT;
		}

		/**
		 * Specify timeout.
		 * <p>
		 * A timeout value of <code>0</code> means never timeout.
		 * 
		 * @param timeoutMillisec	transaction duration limit, milliseconds
		 */
		public void setTimeout(int timeoutMillisec) {
			mTimeout = timeoutMillisec;
		}

		// Package-Private
		static boolean wantsRetry(ClOptions opts) {
			return opts != null && opts.mRetryPolicy == RetryPolicy.ONE_SHOT ?
					false : true;
		}
	}

	/**
	 * Container object for policy attributes used in write operations.
	 * <p>
	 * This object is passed in <code>set()</code>, <code>append()</code>,
	 * <code>prepend()</code>, <code>add()</code>, and <code>delete()</code>
	 * calls to specify write options. <code>null</code> may be passed to use
	 * defaults for all options.
	 * <p>
	 * Set <code>expiration</code> and <code>unique</code> directly, and call
	 * <code>set_generation...()</code> methods to override defaults.
	 * <p>
	 * Generation is the number of times a record has been modified (including
	 * creation) on the server. Therefore if a write operation is creating a
	 * record, the expected generation would be <code>0</code>.
	 */
	public static class ClWriteOptions {
		/**
		 * Record is automatically removed from server this many seconds after
		 * write operation.
		 */
		public int expiration;

		/**
		 * Write operation is create-only, will fail if record already exists.
		 */
		public boolean unique;

		// Package-Private
		boolean mUseGeneration;
		boolean mUseGenerationGt;
		boolean mUseGenerationDup;
		int mGeneration;

		/**
		 * Constructor, sets default write options.
		 * <p>
		 * The object is constructed with <code>expiration</code> of
		 * <code>0</code>, <code>unique</code> set <code>false</code>, and no
		 * expected generation.
		 */
		public ClWriteOptions() {
			expiration = 0;
			unique = false;

			mUseGeneration = false;
			mUseGenerationGt = false;
			mUseGenerationDup = false;
			mGeneration = 0;
		}

		/**
		 * Specify expected generation.
		 * <p>
		 * Write operation will fail if generation is set and it's not equal to
		 * the generation on the server.
		 * 
		 * @param generation		expected generation
		 */
		public void set_generation(int generation) {
			mUseGeneration = true;
			mGeneration = generation;
		}

		/**
		 * Specify highest expected generation.
		 * <p>
		 * Write operation will fail if generation is set and it's less than the
		 * generation on the server. (Useful for restore after backup.)
		 * 
		 * @param generation		highest expected generation
		 */
		public void set_generation_gt(int generation) {
			mUseGenerationGt = true;
			mGeneration = generation;
		}

		/**
		 * Specify expected generation, and flag to duplicate if generation is
		 * not equal to that on server.
		 * <p>
		 * Write operation will generate duplicate record if expected generation
		 * is set and it's not the generation on the server.
		 * 
		 * @param generation		expected generation
		 */
		public void set_generation_dup(int generation) {
			mUseGenerationDup = true;
			mGeneration = generation;
		}
	}

	/**
	 * Container object for optional parameters used in scan operations.
	 * <p>
	 * This object is passed in <code>scan...()</code> calls to specify options.
	 * <code>null</code> may be passed to use defaults for all options.
	 */
	public static class ClScanningOptions {
		// Honored by client - work on nodes in parallel or serially:
		private boolean mConcurrentNodes;
		// Honored by client - have multiple threads per node:
		private int mThreadsPerNode;
		// Honored by server - priority of scan:
		private ClScanningPriority mPriority;
		// Honored by server - terminate scan if cluster in fluctuating state:
		private boolean mFailOnClusterChange;

		/**
		 * Constructor, sets default scan options.
		 * <p>
		 * The object is constructed with concurrent nodes scan enabled, one
		 * thread per node, scan priority {@link ClScanningPriority#AUTO}, and
		 * scan termination on cluster changes disabled.
		 */
		public ClScanningOptions() {
			mConcurrentNodes = true;
			mThreadsPerNode = 1;
			mPriority = ClScanningPriority.AUTO;
			mFailOnClusterChange = false;
		}

		/**
		 * Get concurrent nodes scan setting.
		 * 
		 * @return					<code>true</code> if concurrent nodes scan
		 * 							is enabled, <code>false</code> if not
		 */
		public boolean isConcurrentNodes() {
			return mConcurrentNodes;
		}

		/**
		 * Specify concurrent nodes scan setting.
		 * 
		 * @param concurrentNodes	<code>true</code> to enable concurrent nodes
		 * 							scan, <code>false</code> to disable
		 */
		public void setConcurrentNodes(boolean concurrentNodes) {
			mConcurrentNodes = concurrentNodes;
		}

		/**
		 * Get number of client scan threads per node.
		 * 
		 * @return					number of client threads used to scan a node
		 */
		public int getThreadsPerNode() {
			return mThreadsPerNode;
		}

		/**
		 * Specify number of client scan threads per node - not yet supported.
		 * 
		 * @param threadsPerNode	number of client threads used to scan a node
		 */
		public void setThreadsPerNode(int threadsPerNode) {
			mThreadsPerNode = threadsPerNode;
		}

		/**
		 * Get server scan priority setting.
		 * 
		 * @return					{@link ClScanningPriority scan priority} to
		 * 							be used by server
		 */
		public ClScanningPriority getPriority() {
			return mPriority;
		}

		/**
		 * Specify server scan priority setting.
		 * 
		 * @param priority			{@link ClScanningPriority scan priority} to
		 * 							be used by server
		 */
		public void setPriority(ClScanningPriority priority) {
			mPriority = priority;
		}

		/**
		 * Get scan termination setting.
		 * 
		 * @return					<code>true</code> if scan will terminate on
		 * 							cluster change, <code>false</code> if not
		 */
		public boolean isFailOnClusterChange() {
			return mFailOnClusterChange;
		}

		/**
		 * Specify scan termination setting.
		 * 
		 * @param failOnClusterChange	<code>true</code> to terminate scan on
		 * 							cluster change, <code>false</code> to
		 * 							continue scan
		 */
		public void setFailOnClusterChange(boolean failOnClusterChange) {
			mFailOnClusterChange = failOnClusterChange;
		}
	}

	//-------------------------------------------------------
	// Results of Operations
	//-------------------------------------------------------

	/**
	 * Container object for database operation results.
	 */
	public static class ClResult {
		/**
		 * {@link ClResultCode Result code} for operation.
		 */
		public ClResultCode resultCode = ClResultCode.NOT_SET;

		/**
		 * How many times the record has been modified (including creation) on
		 * the server.
		 */
		public int generation = -1;

		/**
		 * For scans, to notify that the data is corrupted.
		 */
		public boolean corruptedData = false;

		/**
		 * When only one bin is requested, value is returned here.
		 */
		public Object result = null;

		/**
		 * Requested bins are returned here, as name/value pairs.
		 */
		public Map<String, Object> results = null;

		/**
		 * When conflicting versions are encountered, every version's bins are
		 * returned as an element of this <code>List</code>.
		 * <p>
		 * @see					ClWriteOptions#mUseGenerationDup
		 */
		public List<Map<String, Object>> results_dup = null;

		// Package-Private
		// Used internally by batch operations to store per-node results.
		ClResult[] resultArray = null;

		/**
		 * Constructor, creates "empty" object.
		 */
		public ClResult() {
		}

		/**
		 * Constructor, specifies result code.
		 * 
		 * @param resultCode		operation {@link ClResultCode result code}
		 */
		public ClResult(ClResultCode resultCode) {
			this.resultCode = resultCode;
		}

		/**
		 * Convert result code to meaningful <code>String</code>.
		 * 
		 * @param resultCode		operation {@link ClResultCode result code}
		 * @return					interpretation of <code>resultCode</code>
		 */
		public static String resultCodeToString(ClResultCode resultCode) {
			switch (resultCode) {
			case OK:
				return "OK";
			case NOT_SET:
				return "result code has not been set";
			case SERVER_ERROR:
				return "server error";
			case TIMEOUT:
				return "timeout";
			case CLIENT_ERROR:
				return "client error";
			case KEY_NOT_FOUND_ERROR:
				return "key not found error";
			case GENERATION_ERROR:
				return "generation error";
			case PARAMETER_ERROR:
				return "parameter error";
			case KEY_EXISTS_ERROR:
				return "key exists error";
			case BIN_EXISTS_ERROR:
				return "bin exists error";
			case SERIALIZE_ERROR:
				return "serialize error";
			case CLUSTER_KEY_MISMATCH:
				return "cluster key mismatch";
			case SERVER_MEM_ERROR:
				return "server memory error";
			case NO_XDS:
				return "XDS product not available";
			case SERVER_NOT_AVAILABLE:
				return "server not available";
			case BIN_TYPE_ERROR:
				return "bin type error";
			case RECORD_TOO_BIG:
				return "record too big";
			case KEY_BUSY:
				return "key busy";
			default:
				return "unknown error " + resultCode;
			}
		}
	}

	//-------------------------------------------------------
	// Container for Bin Name & Value
	//-------------------------------------------------------

	/**
	 * Container object for record bin name/value pair.
	 */
	public static class ClBin {
		/**
		 * Bin name. Current limit is 14 characters.
		 */
		public String name;

		/**
		 * Bin value.
		 */
		public Object value;

		/**
		 * Constructor, sets <code>null</code> bin name and value.
		 */
		public ClBin() {
			name = null;
			value = null;
		}

		/**
		 * Constructor, specifying bin name and value.
		 * 
		 * @param name				bin name, current limit is 14 characters
		 * @param value				bin value
		 */
		public ClBin(String name, Object value) {
			this.name = name;
			this.value = value;
		}
	}

	//=================================================================
	// Private Functions
	//

	//-------------------------------------------------------
	// Logging Helpers
	//-------------------------------------------------------

	private void ClLogInit() {
		if (gSdf == null) {
			gSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
		}
		Log.setLevel(Log.Level.INFO);
		Log.setCallback(this);
	}

	public static void ClLog(ClLogLevel logLevel, String msg) {
		if (logLevel.ordinal() > gLogLevel.ordinal()) {
			return;
		}

		if (gLogCallback != null) {
			gLogCallback.logCallback(logLevel, msg);
			return;
		}

		String date = gSdf != null ? gSdf.format(new Date()) : "";

		System.err.println(date + " CITRUSLEAF [" +
				Thread.currentThread().getId() + "] " + msg);
	}


	//=================================================================
	// Private Nested Interfaces & Classes
	//
	
	private static ScanPolicy getScanPolicy(ClOptions opts, ClScanningOptions scanOpts, boolean noBinData) {
		ScanPolicy policy = new ScanPolicy();
		
		if (opts != null) {
			policy.timeout = opts.mTimeout;
			policy.maxRetries = 0;
		}
		
		if (scanOpts != null) {
			policy.concurrentNodes = scanOpts.mConcurrentNodes;
			policy.failOnClusterChange = scanOpts.mFailOnClusterChange;
			policy.threadsPerNode = scanOpts.mThreadsPerNode;
		}
		policy.includeBinData = ! noBinData;
		policy.scanPercent = 100;
		return policy;
	}
	
	private static WritePolicy getWritePolicy(ClOptions opts, ClWriteOptions wOpts) {
		WritePolicy policy = new WritePolicy();
	
		setPolicy(opts, policy);
		
		if (wOpts != null) {
			policy.expiration = wOpts.expiration;			

			if (wOpts.unique) {
				policy.recordExistsAction = RecordExistsAction.FAIL;
			}
			else if (wOpts.mUseGeneration) {
				policy.generation = wOpts.mGeneration;
				policy.recordExistsAction = RecordExistsAction.EXPECT_GEN_EQUAL;
			}
			else if (wOpts.mUseGenerationGt) {
				policy.generation = wOpts.mGeneration;
				policy.recordExistsAction = RecordExistsAction.EXPECT_GEN_GT;				
			}
			else if (wOpts.mUseGenerationDup) {
				policy.generation = wOpts.mGeneration;
				policy.recordExistsAction = RecordExistsAction.DUPLICATE;				
			}
		}
		return policy;
	}
	
	private static Policy getPolicy(ClOptions opts) {
		Policy policy = new Policy();
		setPolicy(opts, policy);
		return policy;
	}

	private static void setPolicy(ClOptions opts, Policy policy) {		
		if (opts != null) {
			policy.timeout = opts.mTimeout;
			
			if (opts.mRetryPolicy == RetryPolicy.ONE_SHOT) {
				policy.maxRetries = 0;			
			}
			else {
				policy.sleepBetweenRetries = policy.timeout / (policy.maxRetries + 1);
				
				if (policy.sleepBetweenRetries > 2000) {
					policy.sleepBetweenRetries = 2000;
				}
			}
		}
		else {
			policy.timeout = DEFAULT_TIMEOUT;
			policy.maxRetries = 2;			
			policy.sleepBetweenRetries = policy.timeout / (policy.maxRetries + 1);
		}
	}

	private static Key[] getKeys(String namespace, String set, Collection<Object> keys) throws AerospikeException {
		Key[] keyArray = new Key[keys.size()];
		int count = 0;
		
		for (Object key : keys) {
			keyArray[count++] = new Key(namespace, set, key);
		}
		return keyArray;		
	}	
	
	private static Bin[] getBins(Collection<ClBin> bins) throws AerospikeException {
		if (bins == null) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Null bins");
		}
		Bin[] binArray = new Bin[bins.size()];
		int count = 0;
		
		for (ClBin bin : bins) {
			binArray[count++] = new Bin(bin.name, bin.value);
		}
		return binArray;
	}

	private static Bin[] getBins(Map<String, Object> bins) throws AerospikeException {
		if (bins == null) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Null bins");
		}
		Bin[] binArray = new Bin[bins.size()];
		int count = 0;
		
		for (Entry<String,Object> entry : bins.entrySet()) {
			binArray[count++] = new Bin(entry.getKey(), entry.getValue());
		}
		return binArray;
	}

	private static String[] getBinNames(Collection<String> binNames) throws AerospikeException {
		if (binNames == null) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Null bin names");
		}
		String[] binArray = new String[binNames.size()];
		int count = 0;
		
		for (String binName : binNames) {
			binArray[count++] = binName;
		}
		return binArray;
	}
	
	private static ClResult[] getRecordResults(Record[] records) {
		ClResult[] results = new ClResult[records.length];
		
		for (int i = 0; i < records.length; i++) {
			results[i] = getResult(records[i]);
		}
		return results;
	}
	
	private static ClResult[] getKeyResults(boolean[] existsArray) {
		ClResult[] results = new ClResult[existsArray.length];
		
		for (int i = 0; i < existsArray.length; i++) {
			ClResultCode rc = (existsArray[i])? ClResultCode.OK : ClResultCode.KEY_NOT_FOUND_ERROR;
			results[i] = new ClResult(rc);
		}
		return results;
	}

	private static ClResult getResult(Record record)  {
		ClResult clr;

		if (record != null) {
			clr = new ClResult(ClResultCode.OK);
			clr.results = record.bins;
			clr.generation = record.generation;
			clr.results_dup = record.duplicates;
		}
		else {
			clr = new ClResult(ClResultCode.KEY_NOT_FOUND_ERROR);
		}
		return clr;
	}

	private static ClResult[] getResults(AerospikeException ae, int size) {
		ClResult[] array = new ClResult[size];
		ClResult result = new ClResult(getResultCode(ae));
		
		for (int i = 0; i < size; i++) {
			array[i] = result;
		}
		return array;
	}

	private static ClResult getResult(AerospikeException ae) {
		return new ClResult(getResultCode(ae));
	}

	private static Map<String,ClResultCode> getResultCodes(AerospikeException ae, List<String> nodeNames) {
		ClResultCode rc = getResultCode(ae);
		return getResultCodes(rc, nodeNames);
	}

	private static Map<String,ClResultCode> getResultCodes(ClResultCode rc, List<String> nodeNames) {
		Map<String,ClResultCode> map = new HashMap<String,ClResultCode>(nodeNames.size());
		
		for (String nodeName : nodeNames) {
			map.put(nodeName, rc);
		}
		return map;
	}

	private static ClResultCode getResultCode(AerospikeException ae) {
		switch (ae.getResultCode()) {
		case ResultCode.INVALID_NODE_ERROR:
		case ResultCode.PARSE_ERROR:
			return ClResultCode.CLIENT_ERROR;
			
		case ResultCode.SERIALIZE_ERROR:
			return ClResultCode.SERIALIZE_ERROR;
			
		case ResultCode.OK:
			return ClResultCode.OK;
			
		case ResultCode.SERVER_ERROR:
		default:
			return ClResultCode.SERVER_ERROR;
			
		case ResultCode.KEY_NOT_FOUND_ERROR:
			return ClResultCode.KEY_NOT_FOUND_ERROR;
			
		case ResultCode.GENERATION_ERROR:
			return ClResultCode.GENERATION_ERROR;
			
		case ResultCode.PARAMETER_ERROR:
			return ClResultCode.PARAMETER_ERROR;
			
		case ResultCode.KEY_EXISTS_ERROR:
			return ClResultCode.KEY_EXISTS_ERROR;
			
		case ResultCode.BIN_EXISTS_ERROR:
			return ClResultCode.BIN_EXISTS_ERROR;
			
		case ResultCode.CLUSTER_KEY_MISMATCH:
			return ClResultCode.CLUSTER_KEY_MISMATCH;
			
		case ResultCode.SERVER_MEM_ERROR:
			return ClResultCode.SERVER_MEM_ERROR;
			
		case ResultCode.TIMEOUT:
			return ClResultCode.TIMEOUT;
			
		case ResultCode.NO_XDS:
			return ClResultCode.NO_XDS;
			
		case ResultCode.SERVER_NOT_AVAILABLE:
			return ClResultCode.SERVER_NOT_AVAILABLE;
			
		case ResultCode.BIN_TYPE_ERROR:
			return ClResultCode.BIN_TYPE_ERROR;
			
		case ResultCode.RECORD_TOO_BIG:
			return ClResultCode.RECORD_TOO_BIG;
			
		case ResultCode.KEY_BUSY:
			return ClResultCode.KEY_BUSY;
		}
	}
}
