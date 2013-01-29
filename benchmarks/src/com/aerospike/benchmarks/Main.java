package com.aerospike.benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;

public class Main implements Log.Callback {
	
	private static SimpleDateFormat SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private String[] readSettings(String throughput_file) throws IOException {
		Scanner scanner = new Scanner(new FileInputStream(throughput_file), "UTF-8");
		String[] settings = scanner.nextLine().split(",");
		scanner.close();
		return settings;
	}

	private void doInserts() throws InterruptedException {
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);

		// Create N insert tasks
		int ntasks = this.nTasks < this.nKeys ? this.nTasks : this.nKeys;

		for (int i=0 ; i<ntasks; i++) {
			InsertTask it = new InsertTask(this.kvs, this.startKey, this.nKeys, this.keySize, this.nBins, this.timeout, this.objectSpec, this.counters);
			es.execute(it);
		}
		
		int counterold = 0;

		while (this.counters.write.count.get() < this.nKeys) {
			int counterget = this.counters.write.count.get();
			int tps = counterget - counterold;
			System.out.println(" Wrote " + counterget + " elements " + tps + " tps "+ this.counters.write.fail.get() + " fails)");
			counterold = counterget;
			Thread.sleep(1000);
		}

		System.out.println("Wrote " + this.counters.write.count.get() + " elements " + this.counters.write.fail.get() + " fails)");
		es.shutdownNow();
	}

	private void doRWTest() throws InterruptedException {
		// Certain setting can change dynamically based on the throughput file. Because 
		// of this, we pass these variable settings as AtomicIntegers, and via reference. 
		AtomicIntegerArray settingsArr = new AtomicIntegerArray(4);
		settingsArr.set(0, this.throughput);
		settingsArr.set(1, this.readPct);
		settingsArr.set(2, this.singleBinReadPct);
		settingsArr.set(3, this.singleBinUpdatePct);

		// Start RW tasks...
		ExecutorService es = Executors.newFixedThreadPool(this.nThreads);
		for (int i=0 ; i<this.nTasks; i++) {
			RWTask rt;
			if (this.validate) {
				int tstart = this.startKey + ((int) (this.nKeys*(((float) i)/this.nTasks)));
				int tkeys = (int) (this.nKeys*(((float) (i+1))/this.nTasks)) - (int) (this.nKeys*(((float) i)/this.nTasks));
				rt = new RWTask(this.kvs, tkeys, tstart, this.keySize, this.objectSpec, this.nBins, this.cycleType, this.timeout, settingsArr, this.validate, this.runTime, this.counters, this.debug);
			} else {
				rt = new RWTask(this.kvs, this.nKeys, this.startKey, this.keySize, this.objectSpec, this.nBins, this.cycleType, this.timeout, settingsArr, this.validate, this.runTime, this.counters, this.debug);
			}
			es.execute(rt);
		}

		// wait for all the tasks to finish setting up for validation
		if (this.validate) {
			while(counters.loadValuesFinishedTasks.get() < this.nTasks) {
				Thread.sleep(1000);
				//System.out.println("tasks done = "+counters.loadValuesFinishedTasks.get()+ ", g_ntasks = "+g_ntasks);
			}
			// set flag that everyone is ready - this will allow the individual tasks to go
			counters.loadValuesFinished.set(true);
		}

		// Set start time 
		this.counters.start_time = System.currentTimeMillis();

		// Wait for completion
		for (; this.runTime == 0 || this.counters.timeElapsed.get() < this.runTime; this.counters.timeElapsed.incrementAndGet()) {

			long time = System.currentTimeMillis();
			
			int	numWrites = this.counters.write.count.getAndSet(0);
			int failWrites = this.counters.write.fail.getAndSet(0);
			
			int	numReads = this.counters.read.count.getAndSet(0);
			int failReads = this.counters.read.fail.getAndSet(0);
			
			String date = SimpleDateFormat.format(new Date(time));
			System.out.println(date.toString() + " write(tps=" + numWrites + " fail=" + failWrites + ")" +
				" read(tps=" + numReads + " fail=" + failReads + ")" +
				" total(tps=" + (numWrites + numReads) + " fail=" + (failWrites + failReads) + ")"
				);

			if (throughput_file.length() > 0) {
				// set target throughput and read/update percentage based on throughput_file
				try {
					String[] settings = readSettings(throughput_file);
					settingsArr.set(0, Integer.parseInt(settings[0]));
					settingsArr.set(1, Integer.parseInt(settings[1]));
				} catch (Exception e) {
					System.out.println("can't read throughput file!");
				}
			}
			
			Thread.sleep(1000);
		}
	}

	private String[]        hosts;
	private int             port = 3000;
	private String          namespace = "";
    private String          set = "";
	private DBObjectSpec[]  objectSpec;
	private int             nBins = 1;
	private int             nKeys = 0;
	private int             keySize = 0;
	private int             startKey = 0;
	private String          cycleType = "";
	private int             readPct = -1;
	private int             singleBinReadPct = 0;
	private int             singleBinUpdatePct = 0;
	private boolean         writeKeys = false;
	private boolean         validate = false;
	private int             nThreads = 16;
	private int             nTasks = 1;
	private int             throughput = 0;
	private int             timeout = 0;
	private String          throughput_file = "";
	private int             runTime = 0;
	private CLKeyValueStore kvs	 = null;
	private boolean         debug = false;

	private CounterStore counters = new CounterStore();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// create the command line parser
		
		Main test = new Main(args);
		if (test.writeKeys) {
			try {
				test.doInserts(); 
			} catch (Exception e) {
				System.out.println(" Do Inserts - interrupted ");
				if (test.debug) {
					e.printStackTrace();
				}
			}
		} else {
			try {
				test.doRWTest(); 
			} catch (Exception e) {
				System.out.println(" Do RW Test - interrupted ");
				if (test.debug) {
					e.printStackTrace();
				}
			}
		}
	}


		
	public Main(String[] args) {
		CommandLineParser parser = new PosixParser();

		// create the Options
		Options options = new Options();
		options.addOption("h", "hosts", true, "hosts");
		options.addOption("p", "port", true, "port");
		options.addOption("n", "namespace", true, "namespace");
        options.addOption("S", "set", true, "set");
		options.addOption("k", "keys", true, "number of keys");
		options.addOption("l", "keylength", true, "key length");
		options.addOption("b", "bins", true, "number of bins");
		options.addOption("o", "objectSpec", true, "Specification of object types and sizes");
		options.addOption("s", "startkey", true, "start key");
		options.addOption("f", "workloadfile", true, "workload file location");
		options.addOption("w", "workload", true, "cycle type (I for insert, RU for read/update), readpct, singlebinreadpct, singlebinupdatepct");
		options.addOption("g", "throughput", true, "throughput");
		options.addOption("T", "timeout", true, "timeout");
		options.addOption("z", "threads", true, "number of threads");
		options.addOption("D", "debug", true, "enable debug mode");

		Option replicationOpt = new Option("replication", "replicate data");
		options.addOption(replicationOpt);

		Option validateOpt = new Option("validate", "validate data");
		options.addOption(validateOpt);

		Option verboseOpt = new Option("verbose", "verbose output");
		options.addOption(verboseOpt);

		try {
			// parse the command line arguments
			CommandLine line = parser.parse( options, args );
			/*
			if (args.length == 0) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("run_load_test", options);
				System.exit(-1);
			}
			*/
			
			if (line.hasOption("hosts")) {
				this.hosts = line.getOptionValue("hosts").split(",");
			} else {
				this.hosts = new String[1];
				this.hosts[0] = "127.0.0.1";
			}

			if (line.hasOption("port")) {
				this.port = Integer.parseInt(line.getOptionValue("port"));
			} else {
				this.port = 3000;
			}

			if (line.hasOption("namespace")) {
				this.namespace = line.getOptionValue("namespace");
			} else {
				this.namespace = "test";
			}

            if (line.hasOption("set")) {
            	set = line.getOptionValue("set");
            }

			if (line.hasOption("keys")) {
				this.nKeys = Integer.parseInt(line.getOptionValue("keys"));
			} else {
				this.nKeys = 100000;
			}

			if (line.hasOption("keylength")) {
				this.keySize = Integer.parseInt(line.getOptionValue("keylength"));
			}

			if (line.hasOption("bins")) {
				this.nBins = Integer.parseInt(line.getOptionValue("bins"));
			}

			if (line.hasOption("objectSpec")) {
				String[] objectsArr = line.getOptionValue("objectSpec").split(",");
				this.objectSpec = new DBObjectSpec[objectsArr.length];
				for (int i=0; i<objectsArr.length; i++) {
					String[] objarr = objectsArr[i].split(":");
					DBObjectSpec dbobj = new DBObjectSpec();
					dbobj.type = objarr[0].charAt(0);
					if (objarr.length > 1) {
						dbobj.size = Integer.parseInt(objarr[1]);
					}
					this.objectSpec[i] = dbobj;
				}
			} else {
				this.objectSpec = new DBObjectSpec[1];
				DBObjectSpec dbobj = new DBObjectSpec(); 
				dbobj.type = 'I';	// If the object is not specified, it has one bin of integer type
				this.objectSpec[0] = dbobj;
			}

			if (line.hasOption("startkey")) {
				this.startKey = Integer.parseInt(line.getOptionValue("startkey"));
			}

			if (line.hasOption("workloadfile")) {
				this.throughput_file = line.getOptionValue("workloadfile");
			}

			if (line.hasOption("workload")) {
				String[] workloadOpts = line.getOptionValue("workload").split(",");
				this.cycleType = workloadOpts[0];
				if(this.cycleType.equals("I")) {
					this.writeKeys = true;
				}
				if (workloadOpts.length > 1) {
					if (this.cycleType.equals("I")) {
						System.out.println("insert workload expects only one parameter");
						System.exit(-1);
					} else if (this.cycleType.equals("RMU")) {
						System.out.println("read-modify-update workload expects only one parameter");
						System.exit(-1);
					} else if (this.cycleType.equals("RMI")) {
						System.out.println("read-modify-increment workload expects only one parameter");
						System.exit(-1);
					} else if (this.cycleType.equals("RMD")) {
						System.out.println("read-modify-decrement workload expects only one parameter");
						System.exit(-1);
					}

					this.readPct = Integer.parseInt(workloadOpts[1]);
				} else if (this.cycleType.equals("RU")) {
					System.out.println("read-update workload expects only one parameter");
					System.exit(-1);
				}
				if (workloadOpts.length > 2) {
					this.singleBinReadPct = Integer.parseInt(workloadOpts[2]);
					this.singleBinUpdatePct = Integer.parseInt(workloadOpts[3]);
				}
			} else {
				this.cycleType = "RU";
				this.readPct = 90;
			}

			if (line.hasOption("throughput")) {
				this.throughput = Integer.parseInt(line.getOptionValue("throughput"));
			}
			
			if (line.hasOption("timeout")) {
				this.timeout = Integer.parseInt(line.getOptionValue("timeout"));
			}			 

			if (line.hasOption("threads")) {
				this.nThreads = Integer.parseInt(line.getOptionValue("threads"));
			}  

			if (line.hasOption("validate")) {
				this.validate = true;
			}

			if (line.hasOption("runtime")) {
				this.runTime = Integer.parseInt(line.getOptionValue("runtime"));
			}

			if (line.hasOption("debug")) {
				this.debug = true;
			}

		} catch(Exception e) {
			System.out.println( "Unexpected exception:" + e.toString() );
		}

		if (this.nThreads > 1) {
			//this.nTasks = 2*this.nThreads;
			this.nTasks = nThreads;
		}

		System.out.println("load_test against: " +this.hosts[0] + ":"+this.port 
			+ ", namespace: "+this.namespace 
			+ ", num keys: "
			+ this.nKeys+", threads "+this.nThreads
			+ ", read-write ratio: " 
			+ this.readPct + "/" + (100-this.readPct));

		if (this.keySize == 0) {
			this.keySize = (Integer.toString(this.nKeys+this.startKey)).length();
		}

		if (this.keySize < (Integer.toString(this.nKeys+this.startKey)).length()) {
			String errStr = "keylength (-l) must be at least "+(Integer.toString(this.nKeys+this.startKey)).length()+" for "+this.nKeys+" keys";
			if(this.startKey > 0) {
				errStr = errStr + ", starting at "+this.startKey;
			}
			System.out.println(errStr);
			System.exit(-1);
		}

		if (!this.cycleType.equals("I") && !this.cycleType.equals("RU") && !this.cycleType.equals("RMU") && !this.cycleType.equals("RMI") && !this.cycleType.equals("RMD")) {
			System.out.println("workload type must be I (insert), RU (read/update), RMU (read-modify-update), RMI (read-modify-increment), or RMD (read-modify-decrement)");
			System.exit(-1);
		}

		if (this.cycleType.equals("RU") && (this.readPct < 0 || this.readPct > 100)) {
			System.out.println("read-update workload read percentage must be between 0 and 100");
			System.exit(-1);
		}

		if (this.nThreads < 1) {
			System.out.println("number of client threads (-z) must be > 0");
			System.exit(-1);
		}

		if (this.runTime < 0) {
			System.out.println("runtime (-t) must be >= 0");
			System.exit(-1);
		}
		
		Log.Level level = (debug)? Log.Level.DEBUG : Log.Level.INFO;
		Log.setLevel(level);
		Log.setCallback(this);		

		try {
			this.kvs = new CLKeyValueStore(this.hosts[0], this.port, this.namespace, this.set, this.counters);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	@Override
	public void log(Level level, String message) {
		String date = SimpleDateFormat.format(new Date());
		System.out.println(date.toString() + ' ' + level.toString() + 
			" Thread " + Thread.currentThread().getId() + ' ' + message);		
	}
}
