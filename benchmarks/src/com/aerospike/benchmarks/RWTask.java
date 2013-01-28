package com.aerospike.benchmarks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

//
// Always generates random reads
// between start and end
//
public class RWTask implements Runnable {

	Random rgen;
	String database;
	int nKeys;
	int startKey;
	int keySize;
	int nBins;
	DBObjectSpec[] objects;
	String cycleType;
	int timeout;
	AtomicIntegerArray settingsArr;
	boolean validate;
	int runTime;
	Object[] validationValues	= null;
	int[] validationGenerations = null;
	CounterStore counters;
	AtomicInteger timeElapsed;
	CLKeyValueStore kvs;
	WritePolicy writePolicy;
	WritePolicy writePolicyGeneration;
	Policy policy;
	boolean debug = false;

	public RWTask(CLKeyValueStore kvs, int nKeys, int startKey, int keySize, DBObjectSpec[] objects, int nBins, String cycleType, int timeout, AtomicIntegerArray settingsArr, /*int client_num, String clientdir,*/ boolean validate, int runTime, CounterStore counters, boolean debug) {
		this.kvs         = kvs;
		this.nKeys       = nKeys;
		this.startKey    = startKey;
		this.keySize     = keySize;
		this.objects     = objects;
		this.nBins       = nBins;
		this.cycleType   = cycleType;
		this.timeout     = timeout;
		this.settingsArr = settingsArr;
		this.validate    = validate;
		this.runTime     = runTime;
		this.counters    = counters;
		this.debug       = debug;

		this.timeElapsed = counters.timeElapsed;
		this.rgen = new Random(System.nanoTime());
		
		writePolicy = new WritePolicy();
		writePolicy.timeout = timeout;
		
		writePolicyGeneration = new WritePolicy();
		writePolicyGeneration.timeout = timeout;
		writePolicyGeneration.recordExistsAction = RecordExistsAction.EXPECT_GEN_EQUAL;
		writePolicyGeneration.generation = 0;
		
		policy = new Policy();
		policy.timeout = timeout;
	}
	
	/**
	** setupValidation()
	** Read existing values from the database, save them away in our validation arrays
	**/
	private void setupValidation() {
		// load starting values
		for(int i=0; i<this.nKeys; i++) {
			ResponseObj response = kvs.GetValue(policy, Utils.genKey(this.startKey+i, this.keySize));
			this.validationValues[i]      = response.value;
			this.validationGenerations[i] = response.generation;
		}

		// Tell the global counter that this task is finished loading
		this.counters.loadValuesFinishedTasks.incrementAndGet();

		// wait for all tasks to be finished loading
		while(!this.counters.loadValuesFinished.get()) {
			try {
				Thread.sleep(10);
			} catch (Exception e) {
				System.out.println("can't sleep while waiting for all values to load");
			}
		}
	}
	
	/**
	** doRead
	** Read the key at the given index.
	**/
	protected void doRead(int keyIdx, boolean multiBin) {
		String key = Utils.genKey(this.startKey+keyIdx, this.keySize);
		ResponseObj responseObj;

		if (!multiBin) {
			// read one bin, maybe validate
			int bin = 0; // XXX must deal with lien below!
			responseObj = kvs.GetSingleBin(policy, key, Integer.toString(bin));
			if(this.validate) {
				try {
					Object expectedVal;
					Object newVal;
					if (this.validationValues[keyIdx] != null) {
						expectedVal = ((Object[]) this.validationValues[keyIdx])[bin];
					} else {
						expectedVal = null;
					}
					if (responseObj.value != null) {
						newVal = ((Object[]) responseObj.value)[0];
					} else {
						newVal = null;
					}
					if (newVal == null || !newVal.equals(expectedVal)) {  // XXX this equality test is not likely to do what we want... XXX
						this.counters.valueMismatchCnt.incrementAndGet();
						Utils.writeMismatchedKVP(0, "", /*client_num, clientdir,*/ key, expectedVal, newVal);  // XXX handle this
						System.out.println("MISMATCH | original value = "  + expectedVal+", new value = "+newVal);
					} 
				} catch (Exception e) {
					System.out.println("couldn't cast stored value to Object array");
				}
			}
			counters.sbrcounter.getAndIncrement();
		} else {
			// read all bins, maybe validate
			responseObj = kvs.GetValue(policy, key);
			if (this.validate) {
				Object expectedVal    = this.validationValues[keyIdx];
				String expectedValStr = Arrays.toString((Object[]) expectedVal);
				String newValStr      = Arrays.toString((Object[]) responseObj.value);
				if(responseObj.value == null || !expectedValStr.equals(newValStr)) {
					this.counters.valueMismatchCnt.incrementAndGet();
					Utils.writeMismatchedKVP(0, "", /*client_num, clientdir,*/ key, expectedValStr, newValStr);
					System.out.println("MISMATCH | original Val = "+expectedValStr+", new value = "+newValStr);
				} 
			}
		}

		int teget = this.timeElapsed.get();
		if (this.runTime == 0 || (teget >= Math.min(this.runTime/4, Main.timepad) && teget < Math.max(this.runTime*3/4, this.runTime-Main.timepad))) { // XXX what in hell is this? Only do stats for part of the run time?
			this.counters.rcounter.getAndIncrement();
			this.counters.rtdsum.getAndAdd(responseObj.td);
		}
	}
	
	/**
	** doWrite
	** Write the key at the given index
	**/
	protected void doWrite(int keyIdx, boolean multiBin) {
		String key = Utils.genKey(this.startKey+keyIdx, this.keySize);
		Bin[] bins;

		if (this.validate) {
			bins = Utils.genBins(rgen, multiBin ? 1 : this.nBins, objects, this.validationGenerations[keyIdx]+1);
		} else {
			bins = Utils.genBins(rgen, multiBin ? 1 : this.nBins, objects, 0);
		}
		
		long duration = 0;

		if (!multiBin) {
			// write one bin
			int bin = 0 ;
						
			try {
				duration = kvs.SetValue(writePolicy, key, bins);
				
				if (this.validate) {
					if (this.validationValues[keyIdx] != null) {
						Object[] valarr = (Object[]) validationValues[keyIdx];
						valarr[bin] = bins[bin].value;
						this.validationValues[keyIdx] = valarr;
					}
				}
			}
			catch (AerospikeException ae) {
				System.out.println(ae.getMessage());
			}			
			this.counters.sbwcounter.getAndIncrement();
		} else {
			// write all bins
			try {
				duration = kvs.SetValue(writePolicy, key, bins);
				
				if (this.validate) {
					Object[] valarr = new Object[bins.length];
					int i=0;
					for (Bin bin : bins) {
						valarr[i] = bin.value;
						i++;
					}
					this.validationValues[keyIdx] = valarr;
					this.validationGenerations[keyIdx] += 1;
				}
			}
			catch (AerospikeException ae) {
				System.out.println(ae.getMessage());
			}			
		}
		// what is main.timepad?
		int timeElapsed = this.timeElapsed.get();
		if (this.runTime == 0 || (timeElapsed >= Math.min(this.runTime/4, Main.timepad) && timeElapsed < Math.max(this.runTime*3/4, this.runTime-Main.timepad))) {
			this.counters.wcounter.getAndIncrement();
			this.counters.wtdsum.getAndAdd(duration);
		}
	}
	
	/**
	** doIncrement
	** Increment (or decrement, if incrValue is negative) the key at the given index.
	**/
	protected void doIncrement(int keyIdx, int incrValue) {
		// get key
		String key = Utils.genKey(this.startKey+keyIdx, this.keySize);
		
		// set up bin for increment
		Bin[] bins = new Bin[] {new Bin("", incrValue)};
		long duration = 0;
		
		try {
			duration = kvs.IncrementValue(writePolicyGeneration, key, bins);
			
			if (this.validate) {
				Object[] valarr = new Object[this.nBins];
				valarr[0] = incrValue;	
				this.validationValues[keyIdx] = valarr;
				this.validationGenerations[keyIdx] += 1;
			}
		}
		catch (AerospikeException ae) {
			System.out.println(ae.getMessage());
		}

		int timeElapsed = this.timeElapsed.get();
		if (this.runTime == 0 || (timeElapsed >= Math.min(this.runTime/4, Main.timepad) && timeElapsed < Math.max(this.runTime*3/4, this.runTime-Main.timepad))) {
			this.counters.wcounter.getAndIncrement();
			this.counters.wtdsum.getAndAdd(duration);
		}
	}
	
	public void run() {
 
		// if we're going to be validating, load the data.
		if (this.validate) {
			this.validationValues      = new Object[this.nKeys];
			this.validationGenerations = new int[	this.nKeys];
			setupValidation();
		}

		// set up parameters...
		int throughputget         = settingsArr.get(0);
		double readPct            = settingsArr.get(1) / 100.0;	 // XXX oh, this is intuitive...
		double singleBinReadPct   = settingsArr.get(2) / 100.0;
		double singleBinUpdatePct = settingsArr.get(3) / 100.0;

		// Now run...
		while (runTime == 0 || this.timeElapsed.get() < runTime) {	// XXX is this time elapsed thing really what I want to do?
					
			// Get random key
			int curKeyIdx = rgen.nextInt(this.nKeys);
		
			// Check - is it a read or a write?
			double randnum = rgen.nextDouble();
			boolean isWrite = false;
			if (randnum >= readPct) {
				isWrite = true;
			}

			// Single bin or multibin?
			boolean isMultiBin = false;
			randnum = rgen.nextDouble();
			if (isWrite && (randnum < singleBinUpdatePct)) {
				isMultiBin = true; // XXX is this the right logic?
			} else if (randnum < singleBinReadPct) {
				isMultiBin = true; // XXX is this the right logic?
			}		

			// now do the work
			try {
				if (this.cycleType.equals("RU")) {
					if (isWrite) {
						doWrite(curKeyIdx, isMultiBin);
					}else{
						doRead( curKeyIdx, isMultiBin);
					}
				} else if (this.cycleType.equals("RMU") || this.cycleType.equals("RMI") || this.cycleType.equals("RMD")) {
					// read all bins
					doRead(curKeyIdx, true);

					// write all bins
					if (this.cycleType.equals("RMU")) {
						doWrite(curKeyIdx, true);
					} else if (this.cycleType.equals("RMI")) {
						doIncrement(curKeyIdx, 1);
					} else if (this.cycleType.equals("RMD")) {
						doIncrement(curKeyIdx, -1);
					}
				}	 
			} catch (Exception e) {
				if (!this.debug) {
					System.out.println("Exception - " + e.toString());
				}else{
					e.printStackTrace();
				}
			}		 

			// throttle throughput
			long getCounter = counters.tcounter.incrementAndGet();
			if (throughputget != 0) {
				long sleepfor = 0;
				long t = System.nanoTime();
				if (t-this.counters.start_time < getCounter*1000000000/throughputget) {
					sleepfor = (getCounter*1000000000/throughputget - (t-counters.start_time))/1000000;
				} 
				
				try {
					Thread.sleep(sleepfor);
				} catch (Exception e) {
				}
			}
		}
	}

}
