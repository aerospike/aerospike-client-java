/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * Configuration options for the Aerospike benchmark workload.
 *
 * <p>This class encapsulates all configurable parameters for running benchmarks against an
 * Aerospike database using the Picocli command-line framework. It includes options for:
 *
 * <ul>
 *   <li>Namespace and set configuration
 *   <li>Data model settings (bins, object specifications)
 *   <li>Key range and distribution
 *   <li>Workload patterns (read/write ratios, transaction types)
 *   <li>Performance parameters (throughput, transaction limits)
 *   <li>Record lifecycle management (expiration, TTL)
 *   <li>Consistency and availability settings
 *   <li>Retry policies
 *   <li>UDF (User-Defined Function) execution parameters
 * </ul>
 *
 * <p>Each option is annotated with Picocli's {@code @Option} annotation to enable automatic
 * command-line argument parsing and help documentation generation.
 */
public class WorkloadOptions {

	@Spec
	CommandSpec spec;

	@Option(
		names = {"-n", "-namespace", "--namespace"},
		description = "Set the Aerospike namespace for the benchmark.",
		defaultValue = "test")
	private String namespace = "test";

	@Option(
		names = {"-s", "-set", "--set"},
		description = "Set the Aerospike set name.",
		defaultValue = "testset")
	private String set = "testset";

	@Option(
		names = {"-bns", "-batchNamespaces", "--batchNamespaces"},
		description = "Comma separated values for batch namespaces. Default is regular namespace.")
	private String batchNamespaces;

	@Option(
		names = {"-b", "-bins", "--bins"},
		description =
			"Set the number of Aerospike bins. Each bin will contain an object defined with -o. The"
				+ " default is single bin (-b 1).")
	private Integer bins = 1;

	@Option(
		names = { "-bn", "-binNameBase", "--binNameBase" },
		description = 
			"Set the base name for Aerospike bins (default: 'testbin').\n"
			+ "The first bin will be <binNameBase>_1, the second will be\n"
			+ "<binNameBase>_2, <binNameBase>_3 and so on.\n")
	private String binNameBase = "testbin";

	@Option(
		names = {"-o", "-objectSpec", "--objectSpec"},
		description =
			"I | S:<size> | B:<size> | R:<size>:<rand_pct>\n"
				+ "Set the type of object(s) to use in Aerospike transactions. Type can be 'I' "
				+ "for integer, 'S' for string, 'B' for byte[] or 'R' for random bytes. "
				+ "If type is 'I' (integer), do not set a size (integers are always 8 bytes).")
	private String objectSpec;

	@Option(
		names = {"-R", "-random", "--random"},
		description =
			"Use dynamically generated random bin values instead of default static fixed bin values."
				+ " Default: false")
	private boolean random;

	@Option(
		names = {"-mrtSize", "--mrtSize"},
		description =
			"Number of records per multi record transaction.")
	private Long mrtSize;

	@Option(
		names = {"-k", "-keys", "--keys"},
		description =
			"Set the number of keys the client is dealing with. If using an 'insert' workload"
				+ " (detailed below), the client will write this number of keys, starting from value"
				+ " = startkey. Otherwise, the client will read and update randomly across the values"
				+ " between startkey and startkey + num_keys.  startkey can be set using '-S' or"
				+ " '-startkey'. Default: 100000")
	private Long keys = 100000L;

	@Option(
		names = {"-S", "-startkey", "--startkey"},
		description =
			"Set the starting value of the working set of keys. If using an 'insert' workload, the"
				+ " start_value indicates the first value to write. Otherwise, the start_value"
				+ " indicates the smallest value in the working set of keys.")
	private Long startkey;

	@Option(
		names = {"-F", "-keyFile", "--keyFile"},
		description = "File path to read the keys for read operation.")
	private String keyFile;

	@Option(
		names = {"-KT", "-keyType", "--keyType"},
		description = "Type of the key(String/Integer) in the file, default is String")
	private String keyType = "String";

	@Option(
		names = {"-w", "-workload", "--workload"},
		description =
			"I | RU,<percent>[,<percent2>][,<percent3>] | RR,<percent>[,<percent2>][,<percent3>], RMU"
				+ " | RMI | RMD%nSet the desired workload.%n%n   -w I sets a linear 'insert'"
				+ " workload.%n%n   -w RU,80 sets a random read-update workload with 80%% reads and"
				+ " 20%% writes.%n%n      100%% of reads will read all bins.%n%n      100%% of writes"
				+ " will write all bins.%n%n   -w RU,80,60,30 sets a random multi-bin read-update"
				+ " workload with 80%% reads and 20%% writes.%n%n      60%% of reads will read all"
				+ " bins. 40%% of reads will read a single bin.%n%n      30%% of writes will write"
				+ " all bins. 70%% of writes will write a single bin.%n%n   -w RR,20 sets a random"
				+ " read-replace workload with 20%% reads and 80%% replace all bin(s) writes.%n%n    "
				+ "  100%% of reads will read all bins.%n%n      100%% of writes will replace all"
				+ " bins.%n%n   -w RMU sets a random read all bins-update one bin workload with 50%%"
				+ " reads.%n%n   -w RMI sets a random read all bins-increment one integer bin"
				+ " workload with 50%% reads.%n%n   -w RMD sets a random read all bins-decrement one"
				+ " integer bin workload with 50%% reads.%n%n   -w TXN,r:1000,w:200,v:20%%%n%n     "
				+ " form business transactions with 1000 reads, 200 writes with a variation (+/-) of"
				+ " 20%%%n%n")
	private String workload;

	/**
	 * Sets the expiration time for each record in seconds.
	 *
	 * @param value The expiration time in seconds:
	 *              <ul>
	 *                <li>-1: Never expire
	 *                <li>0: Default to namespace expiration time
	 *                <li>&gt;0: Actual given expiration time
	 *              </ul>
	 * @throws ParameterException if the provided value is less than -1
	 */
	@Option(
		names = {"-e", "-expirationTime", "--expirationTime"},
		description =
			"Set expiration time of each record in seconds.\n"
				+ " -1: Never expire\n"
				+ "  0: Default to namespace expiration time\n"
				+ " >0: Actual given expiration time")
	public void setExpirationTime(int value) throws ParameterException {
		if (value < -1) {
			throw new ParameterException(
				getSpec().commandLine(), String.format(Constants.INVALID_EXPIRATION_TIME_MESSAGE, value));
		}
		expirationTime = value;
	}

	private Integer expirationTime = 0;

	/**
	 * Sets the read touch TTL percent.
	 *
	 * <p>This value is expressed as a percentage of the TTL (or expiration) sent on the most recent
	 * write. A read within this interval of the record's end of life will generate a touch operation.
	 *
	 * @param value The read touch TTL percent value (must be between 0 and 100 inclusive)
	 * @throws ParameterException If the provided value is outside the valid range (0-100)
	 */
	@Option(
		names = {"-rt", "-readTouchTtlPercent", "--readTouchTtlPercent"},
		description =
			"Read touch TTL percent is expressed as a percentage of the TTL (or expiration) sent on"
				+ " the most\n"
				+ "recent write such that a read within this interval of the record's end of life"
				+ " will generate a touch.\n"
				+ "Range: 0 - 100.")
	public void setReadTouchTtlPercent(int value) throws ParameterException {
		if (value < 0 || value > 100) {
			throw new ParameterException(
				getSpec().commandLine(),
				String.format(Constants.INVALID_READ_TOUCH_TTL_PERCENT_MESSAGE, value));
		}
		readTouchTtlPercent = value;
	}

	private Integer readTouchTtlPercent;

	@Option(
		names = {"-g", "-throughput", "--throughput"},
		description =
			"Set a target transactions per second for the client. The client should not exceed this "
				+ "average throughput.")
	private Integer throughput;

	@Option(
		names = {"-t", "-transactions", "--transactions"},
		description =
			"Number of transactions to perform in read/write mode before shutting down. "
				+ "The default is to run indefinitely.")
	private Long transactions;

	@Option(
		names = {"-rackId"},
		description = "Set Rack where this benchmark instance resides.  Default: 0")
	private Integer rackId = 0;

	@Option(
		names = {"-maxRetries"},
		description =
			"Maximum number of retries before aborting the current transaction. \n"
				+ "Default for write: 0 (no retries) \n"
				+ "Default for read: 2 (initial attempt + 2 retries = 3 attempts) \n"
				+ "Default for scan/query: 5 (6 attempts. See ScanPolicy.ScanPolicy() comments.)")
	private Integer maxRetries;

	@Option(
		names = {"-sleepBetweenRetries"},
		description =
			"Milliseconds to sleep between retries if a transaction fails and the timeout was not"
				+ " exceeded. Default: 0 (no sleep)")
	private Integer sleepBetweenRetries;

	@Option(
		names = {"-r", "-replica", "--replica"},
		description =
			"Which replica to use for reads.\n\n"
				+ "Values:  master | any | sequence | preferRack.  Default: sequence\n"
				+ "master: Always use node containing master partition.\n"
				+ "any: Distribute reads across master and proles in round-robin fashion.\n"
				+ "sequence: Always try master first. If master fails, try proles in sequence.\n"
				+ "preferRack: Always try node on the same rack as the benchmark first. If no nodes"
				+ " on the same rack, use sequence.\n"
				+ "Use 'rackId' option to set rack.")
	private String replica;

	/**
	 * Set the read consistency level for AP (Availability and Partition tolerance) mode operations.
	 * This determines how many replicas of a record to read before responding to the client.
	 *
	 * @param value The read consistency level: "one" (default) or "all". - "one": Read from a single
	 *              replica - "all": Read from all replicas
	 * @throws ParameterException If the value is not "one" or "all" (case-insensitive)
	 */
	@Option(
		names = {"-readModeAP"},
		description =
			"Read consistency level when in AP mode.\n" + "Values:  one | all.  Default: one")
	public void setReadModeAp(String value) throws ParameterException {
		String valueLc = value.toLowerCase();
		if (!"one".equals(valueLc) && !"all".equals(valueLc)) {
			throw new ParameterException(
				getSpec().commandLine(), String.format(Constants.INVALID_READ_MODE_AP_MESSAGE, value));
		}
		readModeAp = valueLc;
	}

	private String readModeAp;

	/**
	 * Sets the read consistency level for SC (strong consistency) mode operations.
	 *
	 * @param value The read consistency level. Valid values are (case-insensitive):
	 *              <ul>
	 *                <li>session - Ensures consistency within a session
	 *                <li>linearize - Provides linearizable consistency
	 *                <li>allow_replica - Allows reads from replica nodes
	 *                <li>allow_unavailable - Allows reads from potentially unavailable nodes
	 *              </ul>
	 * @throws ParameterException If the provided value is not one of the valid read consistency
	 *                            levels
	 */
	@Option(
		names = {"-readModeSC"},
		description =
			"Read consistency level when in SC (strong consistency) mode.\n"
				+ "Values:  session | linearize | allow_replica | allow_unavailable.  Default:"
				+ " session")
	public void setReadModeSc(String value) throws ParameterException {
		String valueLc = value.toLowerCase();
		if (!"session".equals(valueLc)
			&& !"linearize".equals(valueLc)
			&& !"allow_replica".equals(valueLc)
			&& !"allow_unavailable".equals(valueLc)) {
			throw new ParameterException(
				getSpec().commandLine(), String.format(Constants.INVALID_READ_MODE_SC_MESSAGE, value));
		}
		readModeSc = valueLc;
	}

	private String readModeSc;

	/**
	 * Sets the desired replica consistency guarantee when committing a transaction on the server.
	 *
	 * @param value commit level string value, must be either "all" or "master" (case-insensitive)
	 * @throws ParameterException if an invalid commit level is provided
	 */
	@Option(
		names = {"-commitLevel"},
		description =
			"Desired replica consistency guarantee when committing a transaction on the server.\n"
				+ "Values:  all | master.  Default: all")
	public void setCommitLevel(String value) throws ParameterException {
		String valueLc = value.toLowerCase();
		if (!"all".equals(valueLc) && !"master".equals(valueLc)) {
			throw new ParameterException(
				getSpec().commandLine(), String.format(Constants.INVALID_COMMIT_LEVEL_MESSAGE, value));
		}
		commitLevel = valueLc;
	}

	private String commitLevel;

	@Option(
		names = {"-sendKey"},
		description = "Send key to server for every operation. Default: false")
	private boolean sendKey;

	@Option(
		names = {"-pids", "--partitionIds"},
		description =
			"Specify the list of comma separated partition IDs the primary keys must belong to")
	private String partitionIds;

	@Option(
		names = {"-upn", "-udfPackageName", "--udfPackageName"},
		description = "Specify the package name where the udf function is located")
	private String udfPackageName;

	@Option(
		names = {"-ufn", "-udfFunctionName", "--udfFunctionName"},
		description = "Specify the udf function name that must be used in the udf benchmarks")
	private String udfFunctionName;

	@Option(
		names = {"-ufv", "-udfFunctionValues", "--udfFunctionValues"},
		description = "The udf argument values comma separated")
	private String udfFunctionValues;

	public CommandSpec getSpec() {
		return spec;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getBatchNamespaces() {
		return batchNamespaces;
	}

	public String getSet() {
		return set;
	}

	public Long getMrtSize() {
		return mrtSize;
	}

	public Long getKeys() {
		return keys;
	}

	public Integer getBins() {
		return bins;
	}

	public String getBinNameBase() {
		return binNameBase;
	}

	public String getObjectSpec() {
		return objectSpec;
	}

	public boolean isRandom() {
		return random;
	}

	public String getKeyFile() {
		return keyFile;
	}

	public String getKeyType() {
		return keyType;
	}

	public Long getStartKey() {
		return startkey;
	}

	public String getWorkload() {
		return workload;
	}

	public Integer getExpirationTime() {
		return expirationTime;
	}

	public boolean isExpirationTimeDefault() {
		return expirationTime == 0;
	}

	public Integer getReadTouchTtlPercent() {
		return readTouchTtlPercent;
	}

	public Integer getThroughput() {
		return throughput;
	}

	public Long getTransactions() {
		return transactions;
	}

	public Integer getRackId() {
		return rackId;
	}

	public Integer getMaxRetries() {
		return maxRetries;
	}

	public Integer getSleepBetweenRetries() {
		return sleepBetweenRetries;
	}

	public String getReplica() {
		return replica;
	}

	public String getReadModeAp() {
		return readModeAp;
	}

	public String getReadModeSc() {
		return readModeSc;
	}

	public String getCommitLevel() {
		return commitLevel;
	}

	public boolean isSendKey() {
		return sendKey;
	}

	public String getPartitionIds() {
		return partitionIds;
	}

	public String getUdfPackageName() {
		return udfPackageName;
	}

	public String getUdfFunctionName() {
		return udfFunctionName;
	}

	public String getUdfFunctionValues() {
		return udfFunctionValues;
	}
}
