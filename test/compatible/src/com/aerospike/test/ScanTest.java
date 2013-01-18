/*
 *  Copyright 2012 by Aerospike, Inc.  All rights reserved.
 *  
 *  Availability of this source code to partners and customers includes
 *  redistribution rights covered by individual contract. Please check
 *  your contract for exact rights and responsibilities.
 */
package com.aerospike.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import net.citrusleaf.CitrusleafClient;
import net.citrusleaf.CitrusleafClient.ClBin;
import net.citrusleaf.CitrusleafClient.ClOptions;
import net.citrusleaf.CitrusleafClient.ClResult;
import net.citrusleaf.CitrusleafClient.ClResultCode;
import net.citrusleaf.CitrusleafClient.ClScanningOptions;
import net.citrusleaf.CitrusleafClient.ScanCallback;

public class ScanTest {
	static private String ns;
	private static class SimpleScanObject
	{
		public int digest_errors = 0;
		public int domain_errors = 0;
		public int n_objects;
		public Set<byte[]> set = new HashSet<byte[]>();
		public Set<String> set_domains = new HashSet<String>();
	}

	public static class SimpleScanCallbackClass implements ScanCallback {
		public void scanCallback(String namespace, String set, byte [] digest, Map<String, Object> values, int generation, int ttl, Object scan_object)
		{
			SimpleScanObject so = (SimpleScanObject)scan_object;
			so.set.add(digest);
			so.n_objects++;
			String val=null;

			if (values == null)
			{
				// going to allow this error past for now
				// so.failed = true;
				System.out.println("there are no bins for this digest ");
				return;
			} else {
				Collection <String> binNames = values.keySet();
				for (String bin: binNames)
				{
					if (bin.equals("domain"))
					{
						try
						{
							val = (String) values.get(bin);
							//System.out.println("bin: "+bin + " value: "+val);
							so.set_domains.add(val);
						} catch (Exception e) {
							System.out.println("can't get value for bin "+bin);
							continue;
						}
					}
				}
			}

			try
			{
				Thread.sleep(5);
			} catch (Exception e) {
				System.out.println("woken up from sleep");
			}
			if (so.n_objects!=(so.set.size()+so.digest_errors)) {
				so.digest_errors++;
				System.out.println("arg! digest map out of sync!"+so.n_objects+":"+so.set.size());
				if (val != null) {
					System.out.println("Domain is :"+val);
				}
				System.out.print("Digest is :");
				for (int i=0; i<20; i++) {
					System.out.print("-"+digest[i]);
				}
				System.out.println("");
			}

			if (so.n_objects!=(so.set_domains.size()+so.domain_errors)) {
				so.domain_errors++;
				System.out.println("arg! domains map out of sync!"+so.n_objects+":"+so.set_domains.size());
				if (val != null) {
					System.out.println("Domain is :"+val);
				}
				System.out.print("Digest is :");
				for (int i=0; i<20; i++) {
					System.out.print("-"+digest[i]);
				}
				System.out.println("");
			}
			if (so.n_objects%100 ==0)  {
				System.out.println("objects callback "+so.n_objects);
			}
		}
	}        

	private static class ScanObject
	{
		public int n_objects;
		public ArrayList <String> startValue;
		public ArrayList <String> startBin;
		public boolean failed;
		public boolean delay;
		public boolean nobindata;
		public int n_keys;
		public int num_bins;
		public boolean[] key_found;
	}

	public static class Class1 implements ScanCallback{
		public void scanCallback(String namespace, String set, byte [] digest, Map<String, Object> values, int generation, int ttl, Object scan_object)
		{
			ScanObject so = (ScanObject)scan_object;
			int i;
			
			/* This is to introduce delay in scan callback function.
			* This is to done to test the functionality that the server does
			* not close the connection used by scan.
			*/
			if (so.delay == true) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					System.out.println("Exception in callback "+e);
					return;
				}
			}

			if (so.nobindata == true) {
				if (values != null) {
					System.err.println("Not expecting bins but got them. Bincount="+values.size());
					so.failed = true;
				} else {
					so.n_objects++;
				}
				return;
			}

			if (values == null) {
				//There can be keys with no bins left from previous testcases
				//System.err.println("Did not get any bins");
				//so.failed = true;
				return;
			}

			if (values.size() != so.num_bins) {
				return;
			}

			// sometimes these tests start with some dirty values in the system.
			// thus, don't freak if you see something that's not yours - just make sure
			// everything I *did* insert is there.
			//System.out.print("\nDigest - "+digest);
			Collection <String> keys = values.keySet();
			for (String key: keys)
			{
				String bin;
				String val;
				try
				{
					bin = (String) key;
					val = (String) values.get(key);
				} catch (Exception e) {
					continue;
				}

				//System.out.print(" : Bin - "+bin+", Value - "+val);
				if (! namespace.equals(ns)) {
					System.err.println("namespace should be "+ns+", but is "+namespace);
					so.failed = true;
					return;
				}

				for (i = 0; i < so.startBin.size(); i++) {
					if (bin.startsWith(so.startBin.get(i)) == true) {
						break;
					}
				}
				if (i == so.startBin.size()) {
				//	System.err.println("bin - "+bin);
					continue;
				}

				for (i = 0; i < so.startValue.size(); i++) {
					if ((val.startsWith(so.startValue.get(i)) == true) || (val.length() == 0)) {
						break;
					}
				}
				if (i == so.startValue.size()) {
				//	System.err.println("value - "+val);
					continue;
				}

				if (generation != so.num_bins) {
					System.err.println("generation should be "+so.num_bins+", but is "+generation);
					so.failed = true;
					return;
				}
				so.n_objects++;

				if (so.num_bins == 1) {
					String i_str = val.substring(so.startValue.get(0).length());
					i = Integer.parseInt(i_str);
					if (i > so.n_keys || i < 0)
					{
						System.err.println(" scan: index " +i+ " out of range");
						so.failed = true;
						return;
					}
					if (so.key_found[i] == true)
					{
						System.err.println(" scan: index " +i+ " found twice");
						so.failed = true;
						return;
					}
					so.key_found[i] = true;
				}
			}
		}
	}
	public static class SynchronizedScanCallback implements ScanCallback{
		public void scanCallback(String namespace, String setname, byte [] digest, Map<String, Object> values, int generation, int ttl, Object scan_object)
		{
			ScanObject so = (ScanObject)scan_object;

			/* This is to introduce delay in scan callback function.
			* This is to done to test the functionality that the server does
			* not close the connection used by scan.
			*/
			if (so.delay == true) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					System.out.println("Exception in callback "+e);
					return;
				}
			}
	
			if (so.nobindata == true) {
				synchronized (so) {
					if (values != null) {
						System.err.println("Not expecting bins but got them. Bincount="+values.size());
						so.failed = true;
					} else {
						so.n_objects++;
					}
				}
				return;
			}
	
			if (values == null) {
				//There can be keys with no bins left from previous testcases
				System.err.println("Did not get any bins");
				//so.failed = true;
				return;
			}

			if (values.size() != so.num_bins) {
				System.err.println("Wrong number of bins?");
				return;
			}
	
			Collection <String> keys = values.keySet();
			synchronized (so) {
				so.n_objects += keys.size();
			}			
		}
	}

	// this is a more basic test with a lot of flushing in between
	public static boolean basicScanTest(Main.Parameters params, boolean delay) {

		CitrusleafClient cc = params.cc;
		ClResultCode rc;
		ClResult cr;
		int num_keys = 60, i;
		String origKey = "myaddkey";
		String origBin = "mybin";
		String origVal = "mytestvalue";

		ns = params.namespace;
		String set = params.set;
		String addKey, bin, Val;

		for (i=0; i<num_keys; i++) {
			addKey = origKey + i;
			bin = origBin+i;
			Val = origVal+i;

			rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			if (rc != ClResultCode.OK) {
				System.out.println("couldn't do initial set "+rc);
				return(false);		
			}
		}
	
		CitrusleafClient.ScanCallback vc = new Class1();
		ScanObject so = new ScanObject();
		so.n_keys = num_keys;
		so.key_found = new boolean[num_keys];
		so.failed = false;
		so.delay = delay;
		so.n_objects = 0;
		so.num_bins = 1;
		so.startValue = new ArrayList<String>();
		so.startBin = new ArrayList<String>();
		so.startValue.add(origVal);
		so.startBin.add(origBin);
		so.nobindata = false;
		cr = cc.scan(ns, set, null, vc, so);
		for (i = 0; i<num_keys; i++) {
			addKey = origKey + i;
			cc.delete(ns, set, addKey, null, null);
		}
		if (cr.resultCode != ClResultCode.OK)
		{
			System.out.println(" citrusleaf scan: failed with error " + cr.resultCode);
			return (false);
		}

		if (so.failed == true) {
			System.out.println(" scan: validation of the scan data failed");
			return (false);
		}

//		System.out.println("Scan called "+so.n_objects+" times");

		if (so.n_objects < num_keys)
		{
			System.out.println(" scan: too few objects found, expecting " + num_keys + "found " + so.n_objects);
			return (false);
		}

		int j = 0;
		for (boolean b : so.key_found)
		{
			if (b != true)
			{
				System.out.println(" scan: index " + j + " not found" + j);
				return (false);
			}
			j++;
		}
		return(true);
	}
 
	// this is a simple scan test frame work without any checking
	public static boolean test2(Main.Parameters params) {

		CitrusleafClient cc = params.cc;
		ClResult cr;

		ns = params.namespace;
		String set = params.set;

		CitrusleafClient.ScanCallback vc = new SimpleScanCallbackClass();
		SimpleScanObject so = new SimpleScanObject();
		cr = cc.scan(ns, set, null, vc, so);
		if (cr.resultCode != ClResultCode.OK)
		{
			System.out.println(" citrusleaf scan: failed with error " + cr.resultCode);
			return (false);
		}
		System.out.println(" scan: number of objects found " + so.n_objects);
		return(true);
	}

	public static boolean testScanRetry(Main.Parameters params) {

		CitrusleafClient cc = params.cc;
		ClResult cr;

		ns = params.namespace;
		String set = params.set;

		CitrusleafClient.ScanCallback vc = new SimpleScanCallbackClass();
		SimpleScanObject so = new SimpleScanObject();
		ClOptions clo = new ClOptions();
		cr = cc.scan(ns, set, clo, vc, null);
		if (cr.resultCode != ClResultCode.CLIENT_ERROR)
		{
			System.out.println(" citrusleaf scan: failed with error " + cr.resultCode);
			return (false);
		}
		System.out.println(" scan: number of objects found " + so.n_objects);
		return(true);
	}

	// this is a scan test with multibin and values having multi byte utf8 enoded Strings and empty String
	public static boolean multiBinUtf8ScanTest(Main.Parameters params) {

		CitrusleafClient cc = params.cc;
		ClResultCode rc;
		ClResult cr;
		Vector<ClBin> vv;
		int num_keys = 1000, i;
		String origKey = "myaddkey";
		String origBin = "mybin";
		String origVal1 = "mytestvalue";
		String origVal2 = new String("A" + "\u00ea" + "\u00f1" + "\u00fc" + "\u20ac" + "C");
		ns = params.namespace;
		String set = params.set;
		String addKey, bin, Val;

		for (i=0; i<num_keys; i++) {
			addKey = origKey + i;
			bin = origBin+i;
			if (i%3 == 0) {
				Val = origVal1+i;
				rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			} else if (i%3 == 1) {
				Val = origVal2+i;
				rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			} else {
				rc = cc.set(ns, set, addKey, bin, "", null, null);		
			}
			if (rc != ClResultCode.OK) {
				System.out.println("couldn't do initial set "+rc);
				return(false);		
			}
		}

		ScanObject so = new ScanObject();
		so.startValue = new ArrayList<String>();
		so.startBin = new ArrayList<String>();
		so.startValue.add(origVal1);
		so.startBin.add(origBin);
		so.nobindata = false;

		origBin = "otherbin";
		origVal1 = "othertestvalue";
		for (i=0; i<num_keys; i++) {
			addKey = origKey + i;
			bin = origBin+i;
			if (i%3 == 1) {
				Val = origVal1+i;
				rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			} else if (i%3 == 2) {
				Val = origVal2+i;
				rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			} else {
				rc = cc.set(ns, set, addKey, bin, "", null, null);		
			}
			if (rc != ClResultCode.OK) {
				System.out.println("couldn't do initial set "+rc);
				return(false);		
			}
		}

		CitrusleafClient.ScanCallback vc = new Class1();
		so.n_keys = num_keys;
		so.key_found = new boolean[num_keys];
		so.failed = false;
		so.delay = false;
		so.n_objects = 0;
		so.num_bins = 2;
		so.startValue.add(origVal1);
		so.startValue.add(origVal2);
		so.startBin.add(origBin);
		cr = cc.scan(ns, set, null, vc, so);
		for (i = 0; i<num_keys; i++) {
			addKey = origKey + i;
			cc.delete(ns, set, addKey, null, null);
		}

		if (cr.resultCode != ClResultCode.OK)
		{
			System.out.println(" citrusleaf scan: failed with error " + cr.resultCode);
			return (false);
		}

		if (so.failed == true) {
			System.out.println(" scan: validation of the scan data failed");
			return (false);
		}
		if (so.n_objects < num_keys)
		{
			System.out.println(" scan: too few objects found, expecting " + num_keys + "found " + so.n_objects);
			return (false);
		}

		if (so.n_objects < 2 * num_keys)
		{
			System.out.println(" scan: too few objects found, expecting " + num_keys + "found " + so.n_objects);
			return (false);
		}

		return(true);
	}
  
	public static boolean scanDigestTest(Main.Parameters params) {

		CitrusleafClient cc = params.cc;
		ClResultCode rc;
		ClResult cr;
		int num_keys = 1000, i;
		String origKey = "myaddkey";
		String origBin = "mybin";
		String origVal = "mytestvalue";

		ns = params.namespace;
		String set = params.set;
		String addKey, bin, Val;

		for (i=0; i<num_keys; i++) {
			addKey = origKey + i;
			bin = origBin+i;
			Val = origVal+i;

			rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			if (rc != ClResultCode.OK) {
				System.out.println("couldn't do initial set "+rc);
				return(false);		
			}
		}
	
		CitrusleafClient.ScanCallback vc = new Class1();
		ScanObject so = new ScanObject();
		so.n_keys = num_keys;
		so.key_found = new boolean[num_keys];
		so.failed = false;
		so.delay = false;
		so.n_objects = 0;
		so.num_bins = 1;
		so.startValue = new ArrayList<String>();
		so.startBin = new ArrayList<String>();
		so.startValue.add(origVal);
		so.startBin.add(origBin);
		so.nobindata = true;
		cr = cc.scan(ns, set, null, vc, so, true);
		for (i = 0; i<num_keys; i++) {
			addKey = origKey + i;
			cc.delete(ns, set, addKey, null, null);
		}
		if (cr.resultCode != ClResultCode.OK)
		{
			System.out.println(" citrusleaf scan: failed with error " + cr.resultCode);
			return (false);
		}

		if (so.failed == true) {
			System.out.println(" scan: validation of the scan data failed");
			return (false);
		}

		if (so.n_objects < num_keys)
		{
			System.out.println(" scan: too few objects found, expecting " + num_keys + "found " + so.n_objects);
			return (false);
		}

		return(true);
	}

	public static boolean concurrentNodeScanTest(Main.Parameters params) {

		CitrusleafClient cc = params.cc;
		ClResultCode rc;
		int num_keys = 2000, i;
		String origKey = "singleNode";
		String origBin = "bbin";
		String origVal = "bval";

		ns = params.namespace;
		String set = "aset";//params.set;
		String addKey, bin, Val;

		// inserting the keys
		for (i=0; i<num_keys; i++) {
			addKey = origKey + i;
			bin = origBin+i;
			Val = origVal+i;

			rc = cc.set(ns, set, addKey, bin, Val, null, null);		
			if (rc != ClResultCode.OK) {
				System.out.println("couldn't do initial set "+rc);
				return(false);		
			}
		}

		CitrusleafClient.ScanCallback scan_callbk = new SynchronizedScanCallback();
		ScanObject so = new ScanObject();
		so.n_keys = num_keys;
		so.key_found = new boolean[num_keys];
		so.failed = false;
		so.delay = false;
		so.n_objects = 0;
		so.num_bins = 1;
		so.startValue = new ArrayList<String>();
		so.startBin = new ArrayList<String>();
		so.startValue.add(origVal);
		so.startBin.add(origBin);
		so.nobindata = false;

		// doing the scan
		System.out.println(" scan: ready to scan all nodes");
		ClScanningOptions scan_options = new ClScanningOptions();
		scan_options.setConcurrentNodes(true);
		
		cc.scanAllNodes(ns, set, null, scan_options, scan_callbk, so);
		if (so.failed == true) {
			System.out.println(" scan: validation of the scan data failed");
			return (false);
		}
		if (so.n_objects < num_keys)
		{
			System.out.println(" scan: too few objects found, expecting " + num_keys + "found " + so.n_objects);
			return (false);
		}

		return(true);
	}
	
	public static
	boolean execute(Main.Parameters params) {		
		if (false == concurrentNodeScanTest(params)) {
			System.out.println("concurrentScanNode test failed");
			return(false);
		} else {
			System.out.println(" concurrentScanNode test succeeded!");
		}
		return true;
	}
}
    
