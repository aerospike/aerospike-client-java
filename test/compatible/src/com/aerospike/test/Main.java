/*
 *  Copyright 2012 by Aerospike, Inc.  All rights reserved.
 *  
 *  Availability of this source code to partners and customers includes
 *  redistribution rights covered by individual contract. Please check
 *  your contract for exact rights and responsibilities.
 */
package com.aerospike.test;

import java.util.*;

import org.apache.commons.cli.*;

import net.citrusleaf.CitrusleafClient;
import net.citrusleaf.CitrusleafClient.*;
import net.citrusleaf.CitrusleafInfo;

public class Main {

	static Random r = new Random();

	// Static helper class for generating KVP from Integers
	public static class KVP {
		
		KVP(int i) {
//	        String s = Integer.toString(i);
	        String s = iToS(i);
			this.key = s;
	        StringBuilder sb = new StringBuilder(200);
	        for (int j=0;j<10;j++) {
	            sb.append(this.key);
	        }
			this.value = sb.toString();
			
		}

		
		KVP() {
//	        String s = Integer.toString(i);
			int i = r.nextInt(2000000);
	        String s = iToS(i);
			this.key = s;
	        StringBuilder sb = new StringBuilder(200);
	        for (int j=0;j<10;j++) {
	            sb.append(this.key);
	        }
			this.value = sb.toString();
			
		}
		
		String key;
		String value;
	}

    public static String iToS(int i) {
    	return ( String.format("%010d",i) );
    }
// Old way was pretty quick    	
//        String s = Integer.toString(i);
//        switch (s.length()) {
//            case 0:  s = "0000000000"; break;
//            case 1:  s = "000000000"+s; break;
//            case 2:  s = "00000000"+s; break;
//            case 3:  s = "0000000"+s; break;
//            case 4:  s = "000000"+s; break;
//            case 5:  s = "00000"+s; break;
//            case 6:  s = "0000"+s; break;
//            case 7:  s = "000"+s; break;
//            case 8:  s = "00"+s; break;
//            case 9:  s = "0"+s; break;
//            default: break;
//        }
//        return(s);



	//
	// Tests!
	//

	public static boolean setAndExpireTest(Parameters params) {
		System.out.println("starting setAndExpire functional test ");
		ClResult cr;
		ClResultCode local_resultCode;
		String key = "setandexpiretest";

		CitrusleafClient cc = params.cc;

		// fetch the original value
		local_resultCode = cc.delete(params.namespace, params.set, key, null, null);
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			if (cr.resultCode == ClResultCode.KEY_NOT_FOUND_ERROR ) {
				// create the record
				System.out.println(" setAndExpireTest: creating new record with key " + key);

				long epoch = System.currentTimeMillis()/1000;
				String newval = "setexpire" + Long.toString(epoch);

				// value to be set
				HashMap<String, Object> vals = new HashMap<String, Object>();
				vals.put(params.bin, newval);

				ClWriteOptions clwo = new ClWriteOptions();
				clwo.expiration = 2;

				local_resultCode = cc.set(params.namespace, params.set, key, vals, null, clwo);
				if (local_resultCode != ClResultCode.OK) {
					System.out.println(" setAndExpireTest: trying to set initial should be OK but got " + local_resultCode);
					return false;
				}
			}
			else {
				System.out.println(" setAndExpireTest: should be OK or KEY_NOT_FOUND_ERROR but got " + cr.resultCode);
				return false;
			}
		}

		// first get, right away
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" get during readback: should be OK but got " + cr.resultCode);
			return false;
		}
		Object newval1 = null;
		if (cr.result != null) {
			newval1 = cr.result;
		}
		else if (cr.results != null) {
			newval1 = cr.results.get(params.bin);
		}
		if (newval1 == null) {
			System.out.println(" getting null pointer for value after get");
			return false;
		}

		System.out.println(" first get, val = " + newval1.toString());

		int sleeptime = 3;
		System.out.println(" now go to sleep for " + sleeptime + " seconds ...");
		try {
			Thread.sleep(sleeptime * 1000);
		}
		catch(Exception ie){
		}

		// second get should find record expired
		cr = cc.getAll(params.namespace, params.set, key, null);
		System.out.println(" second get, result code = " + cr.resultCode);
		if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) {
			System.out.println(" expecting " + ClResultCode.KEY_NOT_FOUND_ERROR + " after expiry");
			return false;
		}

		System.out.println(" setAndExpireTest: completed");
		return true;
    }


    public static boolean getWithTouchTest(Parameters params) {
        System.out.println("starting getWithTouch functional test ");
        ClResult cr;
        ClResultCode local_resultCode;
        String key = "getwithtouchtest";

        CitrusleafClient cc = params.cc;

        // fetch the original value
        local_resultCode = cc.delete(params.namespace, params.set, key, null, null);
        cr = cc.getAll(params.namespace, params.set, key, null);
        if (cr.resultCode != ClResultCode.OK) {
            if (cr.resultCode == ClResultCode.KEY_NOT_FOUND_ERROR ) {
                // create the record
                System.out.println(" getWithTouchTest: creating new record with key "+key);

               long epoch = System.currentTimeMillis()/1000;
               String newval = "gettouch"+Long.toString(epoch);

                // value to be set
		HashMap<String, Object> vals = new HashMap<String, Object>();
		vals.put(params.bin, newval);

		ClWriteOptions clwo = new ClWriteOptions();
                clwo.expiration=2;

                local_resultCode = cc.set(params.namespace, params.set, key, vals, null, clwo);
		if (local_resultCode != ClResultCode.OK) {
                    System.out.println(" getWithTouch: trying to set initial should be OK but got "+local_resultCode);
                    return(false);
		}
            } else {
                System.out.println(" getWithTouchTest: should be OK or KEY_NOT_FOUND_ERROR but got "+cr.resultCode);
                return(false);
            }
        }


        cr = cc.getAll(params.namespace, params.set, key, null);
        if (cr.resultCode != ClResultCode.OK) {
            System.out.println(" get during readback: should be OK but got "+cr.resultCode);
            return(false);
        }
        Object newval1 = null;
        if (cr.result != null) {
            // compare the old with the new
            newval1 = cr.result;
        } else if (cr.results != null) {
            newval1 = cr.results.get(params.bin);
        }
        if (newval1 == null) {
            System.out.println("getting null pointer for value after get");
            return(false);
        }

        int expiration = 5;

        ArrayList<String> clbins = new ArrayList<String>();
        clbins.add(params.bin);

        cr = cc.getWithTouch(params.namespace, params.set, key, clbins, expiration, null);
        System.out.println(" expiration time = "+expiration);
        if (cr.resultCode != ClResultCode.OK) {
            System.out.println(" getWithTouch during readback: should be OK but got "+cr.resultCode);
            return(false);
        }
        Object newval2 = null;
        if (cr.result != null) {
            // compare the old with the new
            newval2 = cr.result;
        } else if (cr.results != null) {
            newval2 = cr.results.get(params.bin);
        }
        if (newval2 == null) {
            System.out.println("getWithTouch: getting null pointer for value after get");
            return(false);
        }


        System.out.println("bin name="+params.bin.toString());
        System.out.println("first get val="+newval1.toString());
        System.out.println("getWithTouch val="+newval2.toString());

        int sleeptime;

        // sleep 
        sleeptime = 2;
        System.out.println("now go to sleep for "+sleeptime+" seconds");
        try{
            Thread.sleep(sleeptime*1000);  //sleep 1000 ms unit
        }
        catch(Exception ie){
        }

        // the read should succeed because it is not expired yet
        cr = cc.getAll(params.namespace, params.set, key, null);
        if (cr.resultCode !=  ClResultCode.OK) {
            System.out.println(" expecting rc= OK got"+cr.resultCode);
        }
        Object newval3 = null;
        if (cr.result != null) {
            newval3 = cr.result;
        } else if (cr.results != null) {
            newval3 = cr.results.get(params.bin);
        }
        if (newval3 == null) {
            System.out.println("getting null pointer for value after get");
            return(false);
        }
        System.out.println("getWithTouch val="+newval3.toString());


        // sleep 
        sleeptime = expiration;
        System.out.println("now go to sleep for "+sleeptime+" seconds");
        try{
            Thread.sleep(sleeptime*1000);  //sleep 1000 ms unit
        }
        catch(Exception ie){
        }

        // the read should fail now with key not found
        cr = cc.getAll(params.namespace, params.set, key, null);
        if (cr.resultCode != ClResultCode.OK) {
            System.out.println(" expecting rc= OK got"+cr.resultCode);
        }
        Object newval4 = null;
        if (cr.result != null) {
            newval4 = cr.result;
        } else if (cr.results != null) {
            newval4 = cr.results.get(params.bin);
        }
        if (newval4 == null) {
            System.out.println("the bin value is not found as expected. This is good");
        }
        if (newval4 != null) {
            System.out.println("getWithTouch val="+newval4.toString());
        }

        System.out.println(" getWithTouch:  completed");
        return true;

    }



	
	public static boolean deleteTest(Parameters params)
	{
		System.out.println(" starting delete functional test ");
		ClResult cr;
		ClResultCode resultCode;
		KVP kvp = new KVP(1);
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, kvp.key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" delete: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		cr =  cc.getAll(params.namespace, params.set, kvp.key, null);
		if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) {
			System.out.println(" delete: should not exist: "+cr.resultCode);
			return(false);
		}
		
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ATestValue", null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" delete: could not set value");
			return(false);
		}
		
		resultCode = cc.delete(params.namespace, params.set, kvp.key, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" delete: fail: result "+resultCode);
			return(false);
		}
		
		cr =  cc.getAll(params.namespace, params.set, kvp.key, null);
		if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) {
			System.out.println(" delete: should not exist: "+cr.resultCode);
			return(false);
		}
		
		System.out.println(" delete: successfully completed");
		
		return(true);
		
	}

	// Try serializing out some complex objects
	public static boolean serializeTest(Parameters params) 
	{
		System.out.println(" starting serialize functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "serializetestserializetestserializetest";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" serialize: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		int[] i_a = new int[10000];
		for (int i=0;i<10000;i++) {
			i_a[i] = i * i;
		}

		// Do a test that pushes this complex object through the serializer
		resultCode = cc.set(params.namespace, params.set, key, params.bin, i_a, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" serialize: should be OK is "+resultCode);
			return(false);
		}

		// Do a test that pushes this complex object through the serializer
		cr = cc.get(params.namespace, params.set, key, params.bin, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" serialize: should be OK is "+resultCode);
			return(false);
		}
		try {
			int[] return_i_a = (int[]) cr.results.get(params.bin);
			if (return_i_a.length != 10000) {
				System.out.println(" serialize: array length should be 10000 ");
				return(false);
			}
			for (int i=0;i<10000;i++) {
				if (return_i_a[i] != i * i) {
					System.out.println(" serialize: value at position "+i+" wrong is "+return_i_a[i]+" should be "+(i*i));
					return(false);
				}
			}
		}
		catch (Exception e) {
			System.out.println(" could not get and cast parameter "+e.toString());
			return(false);
		}
		
		return(true);
	}

	// Negative integers. Try a couple of tests they like.
	public static boolean negIntTest(Parameters params) 
	{
		System.out.println(" starting negInt functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "neginttest";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" negint: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		// let's just try a straight integer negative value
         cc.set(params.namespace, params.set, key, "bin1", Integer.valueOf(-100), null, null);
         cr = cc.get(params.namespace, params.set, key, "bin1", null);
         // System.out.println("set and get integer: should be -100: is "+cr.result);

         Object o = cr.result;
         try {
			 if (Class.forName("java.lang.Integer").isAssignableFrom( o.getClass() ) == true ) {
				 if ((Integer)cr.result != -100) {
					 System.out.println(" negint: int should be -100 ");
					 return(false);
				 }
			}
			else if (Class.forName("java.lang.Long").isAssignableFrom( o.getClass() ) == true ) {
				 if ((Long)cr.result != -100L) {
					 System.out.println(" negint: int should be -100 ");
					 return(false);
				 }
			}
			else {
				 System.out.println(" negint: was not of type integer or long ");
				 return(false);
			}
				
		 }
		 catch (Exception ex) {
			 System.out.println(" negint: threw a strange error "+ex.toString() );
			 return(false);
		 }
		 	 
		
        // let's try a similar long value
        // NOTE: setting a long value , it will usually return as an integer
         cc.set(params.namespace, params.set, key, "bin2", Long.valueOf(-100L), null, null);
         cr = cc.get(params.namespace, params.set, key, "bin2", null);
         // System.out.println("set and get long: should be -100: is "+cr.result);

         o = cr.result;
         try {
			 if (Class.forName("java.lang.Integer").isAssignableFrom( o.getClass() ) == true ) {
				 if ((Integer)cr.result != -100) {
					 System.out.println(" negint: longint should be -100 ");
					 return(false);
				 }
			}
			else if (Class.forName("java.lang.Long").isAssignableFrom( o.getClass() ) == true ) {
				 if ((Long)cr.result != -100L) {
					 System.out.println(" negint: longint should be -100 ");
					 return(false);
				 }
			}
			else {
				 System.out.println(" negint: was not of type integer or long ");
				 return(false);
			}
				
		 }
		 catch (Exception ex) {
			 System.out.println(" negint: threw a strange error "+ex.toString() );
			 return(false);
		 }

         
		return(true);
	}

	// Negative integers. Try a couple of tests they like.
	public static boolean uniqueInsertTest(Parameters params) 
	{
		System.out.println(" starting uniqueInsert functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "uniqueInsert";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" uniqueInsert: delete should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		ClWriteOptions clo = new ClWriteOptions();
		clo.unique = true;
		
		resultCode = cc.set(params.namespace, params.set, key, "bin1", "a value", null, clo);
		if ((resultCode != ClResultCode.OK)) {
			System.out.println(" uniqueInsert: set 1 should be OK is "+resultCode);
			return(false);
		}
		
        cr = cc.get(params.namespace, params.set, key, "bin1", null);
        if (cr.resultCode!=ClResultCode.OK){
			System.out.println(" uniqueInsert: get 1 should be OK is "+resultCode);
			return(false);        	
        }

        resultCode = cc.set(params.namespace, params.set, key, "different bin", "second value", null, clo);
		if ((resultCode != ClResultCode.KEY_EXISTS_ERROR)) {
			System.out.println(" uniqueInsert: set 2 should be KEY_EXISTS_ERROR is "+resultCode);
			return(false);
		} else {
			System.out.println(" uniqueInsert: set 2 did get KEY_EXISTS_ERROR hurray!");			
		}
		return (true);
	}
	
	// Java is annoying in how it deals with types and casts. 'long' is the
	// most sensible integer type, so take any integer-type and upcast
	// Will throw if it's not an integer castable type
	static long
	objectToLong(Object o)
	{
		long l;
		try {
			l = (Long) o;
		}
		catch (Exception e) {
			l = (Integer) o;
		}
		return(l);
	}
	
	// Try various different types and values and see if they all go out correctly
	public static boolean intTest(Parameters params) 
	{
		System.out.println(" starting int functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "inttest";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" int: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		// Test a series of bins\
		HashMap<String, Object> vals = new HashMap<String, Object>();
		vals.put("one",1);
		vals.put("zero", 0);
		vals.put("negone", -1);
		vals.put("bigint", Integer.MAX_VALUE);
		vals.put("smallint", Integer.MIN_VALUE);
		vals.put("biglong", Long.MAX_VALUE);
		vals.put("smalllong", Long.MIN_VALUE);
		vals.put("randOne", r.nextInt(Integer.MAX_VALUE));
		vals.put("randTwo", r.nextInt(Integer.MAX_VALUE));
		vals.put("randThree", r.nextInt(Integer.MAX_VALUE));
		vals.put("randFour", r.nextInt(Integer.MAX_VALUE));
		vals.put("randFive", r.nextInt(Integer.MAX_VALUE));
		
		resultCode = cc.set(params.namespace, params.set, key, vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" int: trying to set int should be OK "+resultCode);
			return(false);
		}
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" int: should be OK is "+resultCode);
			return(false);
		}
		try {
			// check the length
			if (cr.results.size() != vals.size()) {
				System.out.println(" int: sizes should be same: is "+cr.results.size()+" should be "+vals.size());
				return(false);
			}
			
			// compare the old with the new
			for (String bin : vals.keySet()) {
				long should_be = objectToLong( vals.get(bin) );
				long is = objectToLong( cr.results.get(bin) );
				if (should_be != is) {
					System.out.println(" key "+bin+" should be "+should_be+" is "+is );
					return(false);
				}
			}
		}
		catch (Exception e) {
			System.out.println(" could not get and cast parameter "+e.toString());
			return(false);
		}

		
		return(true);
	}

	// Try various different types and values and see if they all go out correctly
	public static boolean blobTest(Parameters params) 
	{
		System.out.println(" starting blob functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "blobtest";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" blob: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		// Create an ordered byte buffer as the first test
		byte[] b = new byte[1024 * 4];
		for (int i=0;i<b.length;i++) {
			b[i] = (byte)i;
		}

		// Push it to the server
		resultCode = cc.set(params.namespace, params.set, key, params.bin, b, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" blob: should be OK is "+resultCode);
			return(false);
		}

		// Pull it back
		cr = cc.get(params.namespace, params.set, key, params.bin, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" blob: should be OK is "+resultCode);
			return(false);
		}
		
		try {
			byte[] b2 = (byte[]) cr.results.get(params.bin);
			if (b2.length != b.length) {
				System.out.println(" blob: bad length: is "+b2.length+" should be "+b.length);
				return(false);
			}
			for (int i=0;i<b.length;i++) {
				if (b2[i] != b[i]) {
					System.out.println(" blob: byte mismatch at: "+i+" should be "+b[i]+" is "+b2[i]);
					return(false);
				}
			}
			
		}
		catch (Exception e) {
			System.out.println(" blob test: some kind of exception "+e);
			return(false);
		}
		return(true);
	}
	
	// Try various different types and values and see if they all go out correctly
	public static boolean largeBlobTest(Parameters params) 
	{
		System.out.println(" starting large blob functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "justbigEnoughtest";
		
		CitrusleafClient cc = params.cc;
		
		resultCode = cc.delete(params.namespace, params.set, key, null, null);

		// Create an ordered byte buffer as the first test
		// assume that the block size is set to 1 meg
		byte[] b = new byte[1024 * 1023];
		//byte[] b = new byte[131072-101];
		for (int i=0;i<b.length;i++) {
			b[i] = (byte)i;
		}

		// Push it to the server
		resultCode = cc.set(params.namespace, params.set, key, params.bin, b, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" blob: should have been ok. Is the block size 1meg?   "+resultCode);
			return(false);
		}

		// Pull it back
		cr = cc.get(params.namespace, params.set, key, params.bin, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" blob: should be OK is "+resultCode);
			return(false);
		}
		
		try {
			byte[] b2 = (byte[]) cr.results.get(params.bin);
			if (b2.length != b.length) {
				System.out.println(" blob: bad length: is "+b2.length+" should be "+b.length);
				return(false);
			}
			for (int i=0;i<b.length;i++) {
				if (b2[i] != b[i]) {
					System.out.println(" blob: byte mismatch at: "+i+" should be "+b[i]+" is "+b2[i]);
					return(false);
				}
			}
			
		}
		catch (Exception e) {
			System.out.println(" blob test: some kind of exception "+e);
			return(false);
		}

		// now do another one that's one byte bigger
		key = "tooBigtest";
		
		// Create an ordered byte buffer as the first test
		// assume that the block size is set to 1 meg
		b = new byte[1024 * 1023+1];
		for (int i=0;i<b.length;i++) {
			b[i] = (byte)i;
		}

		// Push it to the server
		resultCode = cc.set(params.namespace, params.set, key, params.bin, b, null, null);
		if (resultCode != ClResultCode.PARAMETER_ERROR) {
			System.out.println(" blob: should have failed. Is the wblock size 1meg?  "+resultCode);
			return(false);
		}
		return(true);
	}	
	
	// Try various different types and values and see if they all go out correctly
	public static boolean largeBlobMultiTest(Parameters params) 
	{
		System.out.println(" starting multi large blob -functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "largeMultiKey";
		String firstBinName = "firstBinName";
		String secondBinName = "secondBinName";
		
		CitrusleafClient cc = params.cc;
		
		resultCode = cc.delete(params.namespace, params.set, key, null, null);

		// Create an ordered byte buffer as the first test
		// assume that the block size is set to 1 meg
		byte[] b = new byte[1024 * 1023];
		//byte[] b = new byte[131072-101];
		for (int i=0;i<b.length;i++) {
			b[i] = (byte)i;
		}

		// Push it to the server
		resultCode = cc.set(params.namespace, params.set, key, firstBinName, b, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" multi large blob: should have been ok. Is the block size 1meg?   "+resultCode);
			return(false);
		}

		// Pull it back
		cr = cc.get(params.namespace, params.set, key, firstBinName, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" multi large blob: should be OK is "+resultCode);
			return(false);
		}
		
		// Check it
		try {
			byte[] b2 = (byte[]) cr.results.get(firstBinName);
			if (b2.length != b.length) {
				System.out.println(" multi large blob: bad length: is "+b2.length+" should be "+b.length);
				return(false);
			}
			for (int i=0;i<b.length;i++) {
				if (b2[i] != b[i]) {
					System.out.println(" multi large blob: byte mismatch at: "+i+" should be "+b[i]+" is "+b2[i]);
					return(false);
				}
			}
			
		}
		catch (Exception e) {
			System.out.println(" multi large blob test: some kind of exception "+e);
			e.printStackTrace();
			return(false);
		}

		// Push a smaller different bin
		byte[] bshort = new byte[1024];
		for (int i=0;i<bshort.length;i++) {
			bshort[i] = (byte)i;
		}
		resultCode = cc.set(params.namespace, params.set, key, secondBinName, bshort, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" multi large blob: should have succeeded, with log WARNING."+resultCode);
			return(false);
		}
		
		// Pull first one back
		cr = cc.get(params.namespace, params.set, key, firstBinName, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" multi large blob: should be OK is "+resultCode);
			return(false);
		}

		// Check it
		try {
			byte[] b2 = (byte[]) cr.results.get(firstBinName);
			if (b2.length != b.length) {
				System.out.println(" multi large blob: bad length: is "+b2.length+" should be "+b.length);
				return(false);
			}
			for (int i=0;i<b.length;i++) {
				if (b2[i] != b[i]) {
					System.out.println(" multi large blob: byte mismatch at: "+i+" should be "+b[i]+" is "+b2[i]);
					return(false);
				}
			}
		}
		catch (Exception e) {
			System.out.println(" multi large blob test: some kind of exception "+e);
			e.printStackTrace();
			return(false);
		}
		return(true);
	}		
                                                                    
	// Try various different types and values and see if they all go out correctly
	public static boolean nullKeyTest(Parameters params) 
	{
		System.out.println(" starting nullKey functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = null;
		
		CitrusleafClient cc = params.cc;
		
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.PARAMETER_ERROR) ) {
			System.out.println(" nullKey test delete should have failed "+resultCode);
			return(false);
		}
		

		// Push it to the server
		resultCode = cc.set(params.namespace, params.set, key, params.bin, null, null, null);
		if (resultCode != ClResultCode.PARAMETER_ERROR) {
			System.out.println(" nullKey test set should have failed "+resultCode);
			return(false);
		}

		// Pull it back
		cr = cc.get(params.namespace, params.set, key, params.bin, null);
		if (cr.resultCode != ClResultCode.PARAMETER_ERROR) {
			System.out.println(" nullKey test get should have failed "+resultCode);
			return(false);
		}		
		return(true);
	}
	// Try various different types and values and see if they all go out correctly
	public static boolean largeKeyTest(Parameters params) 
	{
		System.out.println(" starting largeKey functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "largekey";
		for (int i=0;i<5;i++) {
			key = key + key;
		}
		String testValue = "oogabooga";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" largeKey: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		// Push it to the server
		resultCode = cc.set(params.namespace, params.set, key, params.bin, testValue, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" largeKey: should be OK is "+resultCode);
			return(false);
		}

		// Pull it back
		cr = cc.get(params.namespace, params.set, key, params.bin, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" largeKey: should be OK is "+resultCode);
			return(false);
		}
		
		try {
			String testValue2 = (String) cr.results.get(params.bin);
			if (testValue2.length() != testValue.length()) {
				System.out.println(" largeKey: bad length: is "+testValue2.length()+" should be "+testValue.length());
				return(false);
			}
			if (testValue.compareTo(testValue2) != 0) {
				System.out.println(" largeKey: value is not correct, is "+testValue2+" should be "+testValue);
				return(false);
			}
			
		}
		catch (Exception e) {
			System.out.println(" largeKey test: some kind of exception "+e);
			return(false);
		}
		return(true);
	}
	
	public static boolean genDuplicateTest(Parameters params)
	{
		System.out.println(" starting DUPLICATE functional test ");
		ClResult cr;
		ClResultCode resultCode;
		KVP kvp = new KVP();
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, kvp.key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" duplicate: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}

		// Do a couple of sets to pump up the generation count
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueOne", null, null);

		cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueTwo", null, null);

		cr =  cc.getAll(params.namespace, params.set, kvp.key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" cs: should exist: "+cr.resultCode);
			return(false);
		}
		try {
			String v = (String) cr.results.get(params.bin);
			if (v.contentEquals("ValueTwo") == false) {
				System.out.println(" gen_dup: have correct value, has "+v+" instead of ValueTwo");
				return(false);
			}
		}
		catch (Exception e) {
			System.out.println(" could not get and cast parameter "+e.toString());
			return(false);
		}
		
        int generation1 = cr.generation;
		// Now, let's do the generation write. It should succeed.
		ClWriteOptions cl_wp = new ClWriteOptions();
		cl_wp.set_generation_dup(generation1);
		
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueThree", null, cl_wp);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" gen_dup: could not set value ");
			return(false);
		}

		// And let's try it again to generate a duplicate
		cl_wp.set_generation_dup(generation1);
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueFour", null, cl_wp);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" gen_dup: got some strange error instead of OK: "+resultCode);
			return(false);
		}

		// we should have two copies
        // [these two should both return the same multi-bin value --- but they don't yet
//		cr = cc.get(params.namespace, params.set, kvp.key, params.bin, null);
		cr = cc.getAll(params.namespace, params.set, kvp.key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" gen_dup: fail: result "+cr.resultCode);
			return(false);
		}
        if (cr.results != null) {
            System.out.println(" gen_dup: should NOT have single result set, should have duplicate results");
            return(false);
        }
        if (cr.results_dup == null) {
            System.out.println(" gen_dup: must have multiple results");
            return(false);
        }
		// and it better be values 3 and 4, precisely
		try {
            boolean has3 = false;
            boolean has4 = false;
            System.out.println(" size of results list: "+cr.results_dup.size() );
            for (Map<String,Object> results : cr.results_dup ) {
                System.out.println(" result map "+results);
                String v = (String) results.get(params.bin);
                if (v.contentEquals("ValueThree")==true) {
                    if (has3 == true) {
                        System.out.println(" gen_dup: have two copies of three, FAIL");
                        return(false);
                    }
                    has3 = true;
                }
                else if (v.contentEquals("ValueFour")==true) {
                    if (has4 == true) {
                        System.out.println(" gen_dup: have two copies of four, FAIL");
                        return(false);
                    }
                    has4 = true;
                }
                else {
                    System.out.println(" gen_dup: got back some result just not expected");
                    return(false);
                }
            }
            if (has3 != true) {
                System.out.println(" gen_dup never got value 3");
                return(false);
            }
            if (has4 != true) {
                System.out.println(" gen_dup: never got value 4");
                return(false);
            }
		}
		catch (Exception e) {
			System.out.println(" gen_dup could not get and cast parameter "+e.toString());
			return(false);
		}
		
		System.out.println(" gen_dup: successfully completed");
		
		return(true);
		
	}
	
	public static boolean generationTest(Parameters params)
	{
		System.out.println(" starting CAS functional test ");
		ClResult cr;
		ClResultCode resultCode;
		KVP kvp = new KVP();
		
		CitrusleafClient cc = params.cc;

		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, kvp.key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" CAS: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}

		// Do a couple of sets to pump up the generation count
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueOne", null, null);

		cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueTwo", null, null);

		cr =  cc.getAll(params.namespace, params.set, kvp.key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" cs: should exist: "+cr.resultCode);
			return(false);
		}
		try {
			String v = (String) cr.results.get(params.bin);
			if (v.contentEquals("ValueTwo") == false) {
				System.out.println(" cs: have correct value, has "+v+" instead of ValueTwo");
				return(false);
			}
		}
		catch (Exception e) {
			System.out.println(" could not get and cast parameter "+e.toString());
			return(false);
		}
		
		// Now, let's do the cas write. It should succeed.
		ClWriteOptions cl_wp = new ClWriteOptions();
		cl_wp.set_generation(cr.generation);
		
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueThree", null, cl_wp);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" CAS: could not set value ");
			return(false);
		}

		// And let's try it again with failure
		cl_wp.set_generation(99999);
		resultCode = cc.set(params.namespace, params.set, kvp.key, params.bin, "ValueFour", null, cl_wp);
		if (resultCode != ClResultCode.GENERATION_ERROR) {
			System.out.println(" CAS: should have gotten generation error, wassup?");
			return(false);
		}

		// and fetch the value back to make sure all is good
		cr = cc.get(params.namespace, params.set, kvp.key, params.bin, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" CAS: fail: result "+cr.resultCode);
			return(false);
		}
		// and it better be value 3 and not value 4
		try {
			String v = (String) cr.results.get(params.bin);
			if (v.contentEquals("ValueThree")==false) {
				System.out.println(" cs: have correct value, has "+v+" instead of ValueTwo");
				return(false);
			}
		}
		catch (Exception e) {
			System.out.println(" CAS could not get and cast parameter ");
			return(false);
		}
		
		System.out.println(" CAS: successfully completed");
		
		return(true);
		
	}
            
	public static boolean simplePrependTest(Parameters params)
	{
		System.out.println(" starting simplePrepend functional test ");
		ClResult cr;
		ClResultCode local_resultCode;
		String key = "simpleprependtest";
		
		CitrusleafClient cc = params.cc;
		// Test a small map

                // No need to set the initial value, since prepend will create the value if it is not already there

                // fetch the original value
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
                    if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR ) {
			System.out.println(" simplePrepend: should be OK but got "+cr.resultCode);
			return(false);
                    }
		}
                Object oldval = null;
                if (cr.result != null) {
                    // compare the old with the new
                    oldval = cr.result;
                } else if (cr.results != null) {
                    oldval = cr.results.get(params.bin);
                }
                if (oldval == null) {
                    oldval = new String("");
                }

                long epoch = System.currentTimeMillis()/1000;
                String newhalf= "newhead"+Long.toString(epoch);

                // prepend the values
		HashMap<String, Object> prependvals = new HashMap<String, Object>();
		prependvals.put(params.bin, newhalf);

		local_resultCode = cc.prepend(params.namespace, params.set, key, prependvals, null, null);
                if (local_resultCode !=  ClResultCode.OK) {
                    System.out.println(" simplePrepend: Failed to simple prepend. Error code = " + local_resultCode);
                    return(false);
                }

                // read back and check
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" simple prepend during readback: should be OK but got "+cr.resultCode);
			return(false);
		}

                Object newval = null;
                if (cr.result != null) {
                    // compare the old with the new
                    newval = cr.result;
                } else if (cr.results != null) {
                    newval = cr.results.get(params.bin);
                }
                if (newval == null) {
                    System.out.println("simple prepend: value is getting null pointer after get");
                    return(false);
                }
			
                // compare the old with the new
                System.out.println("After prepend:");
                System.out.println("oldval="+oldval.toString());
                System.out.println("prependval="+newhalf);
                System.out.println("newval="+newval.toString());

		// leave the record, may be we can use it the next round?

		System.out.println(" simplePrepend test succeeded ");
	
                // should return true on check it!
		return(true);
	}

	// This test checks various combinations of value types, and uses a slightly different interface
	// for the append than the simpleAppendTest
	public static boolean xplusoneAppendTest(Parameters params)
	{
		System.out.println(" starting xplusone append test");
		ClResultCode cr;
		ClResult r;
		String key = "xplusoneappendTest";
		
		CitrusleafClient cc = params.cc;

		// let's first do a delete...
		cr = cc.delete(params.namespace, params.set, key, null, null);
		cr = cc.delete(params.namespace, "newset", key, null, null);

		cr = cc.set(params.namespace, params.set, key, "testBin", "testValue1", null, null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - initial set fails, result " + cr);
			return false;
		}

		// Append a string to a string with a hashmap...
		HashMap<String, Object> myHash = new HashMap<String,Object>();
		myHash.put("testBin", "testValue2");
		cr = cc.append(params.namespace, params.set, key, myHash, null, null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - append string with hashmap fails, result " + cr);
			return false;
		}

		cr = cc.append(params.namespace, params.set, key, "testBin", "testValue3", null, null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - append string to string fails, result " + cr);
			return false;
		}

		// read back result
		String[] bins = new String[]{"testBin"};
		r = cc.get(params.namespace, params.set, key, bins, null);
		if (r.resultCode != ClResultCode.OK || r.results == null || r.results.get("testBin") == null) {
			System.out.println(" xplusoneAppendTest - could not retrieve result!, error " + r.resultCode);
			return false;
		}
		if (!(r.results.get("testBin") instanceof String)) {
			System.out.println(" xplusoneAppendTest - expected string, got " + r.result.getClass().getName());
			return false;
		}
		
		if (!r.results.get("testBin").equals("testValue1testValue2testValue3")) {
			System.out.println(" xplusoneAppendTest - did not get expected value, result is " + r.results.get("testBin"));
			return false;
		}
 
		// and appending an int to a string...
		cr = cc.append(params.namespace, params.set, key, "testBin", 2, null, null);
		if (cr != ClResultCode.PARAMETER_ERROR ) {
			System.out.println(" xplusoneAppendTest - append int to string does not return parameter error, result " + cr);
			return false;
		}
		
		// and appending a blob to a string...
		byte[] blob = new byte[]{0x1, 0x2, 0x3};
		cr = cc.append(params.namespace, params.set, key, "testBin", blob, null, null);
		if (cr != ClResultCode.PARAMETER_ERROR ) {
			System.out.println(" xplusoneAppendTest - append blob to string does not return parameter error, result " + cr);
			return false;
		}

		cr = cc.append(params.namespace, "newset", key, "testBin", "testValue", null, null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - append without initial value fails, result " + cr);
			return false;
		}
	
		r = cc.get(params.namespace, "newset", key, bins, null);
		if (r.resultCode != ClResultCode.OK || r.results == null || r.results.get("testBin") == null){
			System.out.println(" xplusoneAppendTest - could not set new result with append, code " + r.resultCode);
			return false;
		}
		if (!(r.results.get("testBin") instanceof String)) {
			System.out.println(" xplusoneAppendTest - expected string in new value, got " + r.result.getClass().getName());
			return false;
		}

		if (!r.results.get("testBin").equals("testValue")) {
			System.out.println(" xplusoneAppendTest - did not get correct value back when creating via append, result is " + r.results.get("testBin"));
			return false;
		}

		cr = cc.append("badnamespace", params.set, key, "testBin", "testValue", null, null);
		if (cr != ClResultCode.PARAMETER_ERROR ) {
			System.out.println(" xplusoneAppendTest - append to non-existant namespace succeeds, result " + cr);
			return false;
		}

		// okay, now let's try appending to an integer...
		cr = cc.set(params.namespace, params.set, key, "intBin", 2, null, null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - initial set of integer fails, result " + cr);
			return false;
		}

		cr = cc.append(params.namespace, params.set, key, "intBin", "testValue1", null, null);
		if (cr != ClResultCode.PARAMETER_ERROR ) {
			System.out.println(" xplusoneAppendTest - append string to int does not return PARAM error, result " + cr);
			return false;
		}

		cr = cc.append(params.namespace, params.set, key, "intBin", 3, null, null);
		if (cr != ClResultCode.PARAMETER_ERROR) {
			System.out.println(" xplusoneAppendTest - appending an int to an int does not return PARAM error, result " + cr);
			return false;
		}

		byte[] myblob = new byte[]{0x1};
		cr = cc.append(params.namespace, params.set, key, "intBin", myblob, null, null);
		if (cr != ClResultCode.PARAMETER_ERROR) {
			System.out.println(" xplusoneAppendTest - appending a blob to an int does not return PARAM error, result " + cr);
			return false;
		}

		// and now try blobs. Blob on blob should work...
		byte[] initialBlob = new byte[]{0xa, 0xb, 0xc};
		byte[] newBlob = new byte[]{0xd, 0xe, 0xf};
		byte[] resultBlob = new byte[]{0xa, 0xb, 0xc, 0xd, 0xe, 0xf};
		cr = cc.set(params.namespace, params.set, key, "blobBin",initialBlob, null,null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - initial set of blob fails, result " + cr);
			return false;
		}

		cr = cc.append(params.namespace, params.set, key, "blobBin", newBlob, null, null);
		if (cr != ClResultCode.OK ) {
			System.out.println(" xplusoneAppendTest - append blob to blob fails, result " + cr);
			return false;
		}
	
		String[] blobBinArray = new String[]{"blobBin"};
		r = cc.get(params.namespace, params.set, key, blobBinArray, null);
		if (r.resultCode != ClResultCode.OK || r.results == null) {
			System.out.println(" xplusoneAppendTest - append to blob does set expected result " + cr);
			return false;
		}

		if (!(r.results.get("blobBin") instanceof byte[])) {
			System.out.println(" xplusoneAppendTest - append to blob returns incorrect type " + r.results.get("blobBin").getClass().getName());
			return false;
		}

		if (!Arrays.equals((byte[])(r.results.get("blobBin")), resultBlob)) {
			System.out.println(" xplusoneAppendTest - append to blob returns incorrect value " + r.results.get("blobBin"));
			return false;
		}

		cr = cc.append(params.namespace, params.set, key, "blobBin", 1, null, null);
		if (cr != ClResultCode.PARAMETER_ERROR ) {
			System.out.println(" xplusoneAppendTest - append int to blob does not return parameter error, result " + cr);
			return false;
		}

		cr = cc.append(params.namespace, params.set, key, "blobBin", "foo", null, null);
		if (cr != ClResultCode.PARAMETER_ERROR ) {
			System.out.println(" xplusoneAppendTest - append string to blob does not return PARAM error, result " + cr);
			return false;
		}
		 
		return true;
	}

	public static boolean simpleAppendTest(Parameters params)
	{
		System.out.println(" starting simpleAppend functional test ");
		ClResult cr;
		ClResultCode local_resultCode;
		String key = "simpleappendtest";
		
		CitrusleafClient cc = params.cc;

		// Test a small map

                // No need to set the initial value, since append will create the value if it is not already there

                // fetch the original value
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
                    if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR ) {
			System.out.println(" simpleAppend: should be OK but got "+cr.resultCode);
			return(false);
                    }
		}
                Object oldval = null;
                if (cr.result != null) {
                    // compare the old with the new
                    oldval = cr.result;
                } else if (cr.results != null) {
                    oldval = cr.results.get(params.bin);
                }
                if (oldval == null) {
                    oldval = new String("");
                }

                long epoch = System.currentTimeMillis()/1000;
                String newhalf= "newtail"+Long.toString(epoch);

                // append the values
		HashMap<String, Object> appendvals = new HashMap<String, Object>();
		appendvals.put(params.bin, newhalf);

		local_resultCode = cc.append(params.namespace, params.set, key, appendvals, null, null);
                if (local_resultCode !=  ClResultCode.OK) {
                    System.out.println(" simpleAppend: Failed to simple append. Error code = " + local_resultCode);
                    return(false);
                }

                // read back and check
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" simple append during readback: should be OK but got "+cr.resultCode);
			return(false);
		}

                Object newval = null;
                if (cr.result != null) {
                    // compare the old with the new
                    newval = cr.result;
                } else if (cr.results != null) {
                    newval = cr.results.get(params.bin);
                }
                if (newval == null) {
                    System.out.println("simple append: value is getting null pointer after get");
                    return(false);
                }
			
                // compare the old with the new
                System.out.println("After append:");
                System.out.println("oldval="+oldval.toString());
                System.out.println("appendval="+newhalf);
                System.out.println("newval="+newval.toString());

		// leave the record, may be we can use it the next round?

		System.out.println(" simpleAppend test succeeded ");
	
                // should return true on check it!
		return(true);
	}
	
	public static boolean multiBinTest(Parameters params)
	{
		System.out.println(" starting multibin functional test ");
		ClResult cr;
		ClResultCode resultCode;
		String key = "mbtest";
		
		CitrusleafClient cc = params.cc;
		
		// Unknown state of cluster (should either return "key not available" or "ok")
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" multibin: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		
		// Test a small map
		HashMap<String, Object> vals = new HashMap<String, Object>();
		vals.put("o","freakazoid");
		vals.put("zerobab", "!~!12345678");
		
		resultCode = cc.set(params.namespace, params.set, key, vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" multibin: trying to set int should be OK "+resultCode);
			return(false);
		}
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" multibin: should be OK is "+resultCode);
			return(false);
		}
		try {
			// check the length
			if (cr.results.size() != vals.size()) {
				System.out.println(" multibin: sizes should be same: is "+cr.results.size()+" should be "+vals.size());
				return(false);
			}
			
			// compare the old with the new
			for (String bin : vals.keySet()) {
				String should_be = (String) vals.get(bin);
				String is = (String) cr.results.get(bin);
				if (should_be.contentEquals( is ) == false) {
					System.out.println(" key "+bin+" should be "+should_be+
							" is "+is);
					return(false);
				}
			}
		}
		catch (Exception e) {
			System.out.println(" could not get and cast parameter "+e.toString());
			return(false);
		}

		// zero it out
		resultCode = cc.delete(params.namespace, params.set, key, null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" multibin: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}

		// Test a large collection
		ArrayList<ClBin> list = new ArrayList<ClBin>();
		for (int i=0;i<40;i++) {
			list.add( new ClBin( 
					      Integer.toHexString(i), Integer.toBinaryString(i))
			        );
		}

		resultCode = cc.set(params.namespace, params.set, key, list, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" multibin: trying to set int should be OK "+resultCode);
			return(false);
		}
		cr = cc.getAll(params.namespace, params.set, key, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println(" multibin: should be OK is "+resultCode);
			return(false);
		}
		try {
			// check the length
			if (cr.results.size() != list.size()) {
				System.out.println(" multibin: sizes should be same: is "+cr.results.size()+" should be "+list.size());
				return(false);
			}
			
			// compare the old with the new
			for (ClBin bin : list) {
				if (bin.value.equals(cr.results.get(bin.name)) == false) {
					System.out.println(" key "+bin.name+" should be "+bin.value+
							" is "+cr.results.get(bin.name) );
					return(false);
				}
			}
		}
		catch (Exception e) {
			System.out.println(" some aw4eful problem "+e.toString());
			return(false);
		}

		System.out.println(" Multibin test succeeded ");
		
		return(true);
	}
	
	
	
	//
	// Do some calls with the info system
	//
	public static boolean infoTest(Parameters params)
	{
		// Get the entire list first
		System.out.println("info test");
		System.out.println(" --- will spit out the large number of read replicas for a particular node");
		
		HashMap<String, String> infos = CitrusleafInfo.get(params.host,params.port);
		if (infos == null) return(false);
		if (infos.get("node") == null || infos.get("version") == null)	return(false);
		System.out.println(" node value: "+infos.get("node"));
		System.out.println(" version: "+infos.get("version"));
		
		System.out.println(" statistics: "+
				CitrusleafInfo.get(params.host, params.port, "statistics") );
		
		String[] s_a = new String[2];
		s_a[0] = "replicas-read";
		s_a[1] = "replicas-write";
		HashMap<String, String> replica_hash = CitrusleafInfo.get(params.host, params.port, s_a);

		String read_replica_list = replica_hash.get(s_a[0]);
		System.out.println(" replicas-read: "+read_replica_list);
		String[] replicas = read_replica_list.split(";");
		for (String replica : replicas) {
			String[] nv = replica.split(":");
			System.out.println(nv[1]+"  namespace "+nv[0]);
		}
		
		System.out.println(" info test success ");
		
		return(true);
	}

	private static boolean printBatchResult(ClResult[] cr_a, ArrayList<?> keys, ArrayList<?> bins) {
		ClResult cr;
		int keys_size = keys.size();

		if (cr_a == null)
		{
			System.out.println(" batch: NULL result returned");
			return false;
		}
	
		for (int i=0 ; i<keys_size && i<cr_a.length ; i++)
		{
			cr = cr_a[i];
			if (cr == null) {
				System.out.println(" batch: No result for the key "+keys.get(i));
				return false;
			}
			if (cr.resultCode != ClResultCode.OK) {
				System.out.println(" batch: should be OK is "+cr.resultCode);
				return false;
			}
			
			for (Iterator<?> it=bins.iterator(); it.hasNext(); )
			{
				String bin = (String) it.next();
				try {
					try {
						String data = (String) cr.results.get(bin);
						if (data == null) {
							//System.out.println(" batch: No data for bin "+bin);
						} else {
							//System.out.println(" batch: data for bin "+bin+" is "+data);
						}
					} catch (NullPointerException npe) {
						System.out.println(" batch: encounter null pointer for bin. May be 'single-bin true' server config is in effect?");
					}
				}
				catch (Exception e) {
					System.out.println(" batch: could not get and cast parameter "+e.toString());
					return false;
				}
			}
		}

		return true;
	}

	private static boolean insertBatchTestData(Parameters params) {
		ClResultCode resultCode;
		HashMap<String, Object> vals = new HashMap<String, Object>();

		CitrusleafClient cc = params.cc;

		// Insert test data of 5 keys. First delete the keys that may be present from previous run.
		resultCode = cc.delete(params.namespace, params.set, "key1", null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" batch: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		resultCode = cc.delete(params.namespace, params.set, "key2", null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" batch: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		resultCode = cc.delete(params.namespace, params.set, "key3", null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" batch: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		resultCode = cc.delete(params.namespace, params.set, "key4", null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" batch: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}
		resultCode = cc.delete(params.namespace, params.set, "key5", null, null);
		if ((resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) && (resultCode != ClResultCode.OK)) {
			System.out.println(" batch: should be OK or NOT FOUND is "+resultCode);
			return(false);
		}

		vals.clear();
		vals.put("","val1");
		vals.put("bin2", "val1_2");
		resultCode = cc.set(params.namespace, params.set, "key1", vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" batch: trying to set string should be OK "+resultCode);
			return(false);
		}
		vals.clear();
		vals.put("","val2");
		vals.put("bin2", "val2_2");
		resultCode = cc.set(params.namespace, params.set, "key2", vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" batch: trying to set string should be OK "+resultCode);
			return(false);
		}
		vals.clear();
		vals.put("","val3");
		vals.put("bin2", "val3_2");
		resultCode = cc.set(params.namespace, params.set, "key3", vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" batch: trying to set string should be OK "+resultCode);
			return(false);
		}
		vals.clear();
		vals.put("","val4");
		vals.put("bin2", "val4_2");
		resultCode = cc.set(params.namespace, params.set, "key4", vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" batch: trying to set string should be OK "+resultCode);
			return(false);
		}
		vals.clear();
		vals.put("","val5");
		vals.put("bin2", "val5_2");
		resultCode = cc.set(params.namespace, params.set, "key5", vals, null, null);
		if (resultCode != ClResultCode.OK) {
			System.out.println(" batch: trying to set string should be OK "+resultCode);
			return(false);
		}

		return true;
	}

	public static boolean batchTest(Parameters params) 
	{
		ClResult cr_a[];
		boolean rv;
		
		CitrusleafClient cc = params.cc;

		rv = insertBatchTestData(params);
		if (rv == false) {
			return false;
		}

		ArrayList<Object> keys = new ArrayList<Object>();
		keys.add(new String("key1"));
		keys.add(new String("key2"));
		keys.add(new String("key3"));
		keys.add(new String("key5"));

		ArrayList<String> bins = new ArrayList<String>();
		bins.add("");
		bins.add("bin2");
		bins.add("bin3"); //doesn't exist. Adding it to catch exceptions if any

		//Get a single bin
		cr_a = cc.batchGet(params.namespace, params.set, keys, "bin2", null);
		rv = printBatchResult(cr_a, keys, bins);
		if (rv == false) {
			System.out.println("failed 1");
			return false;
		}
		//Get a set of bins
		cr_a = cc.batchGet(params.namespace, params.set, keys, bins, null);
		rv = printBatchResult(cr_a, keys, bins);
		if (rv == false) {
			System.out.println("failed 2");
			return false;
		}
		//Get all the bins
		cr_a = cc.batchGetAll(params.namespace, params.set, keys, null);
		rv = printBatchResult(cr_a, keys, bins);
		if (rv == false) {
			System.out.println("failed 3");
			return false;
		}

		//Do negative testing with keys that are not present
		keys.clear();
		keys.add(new String("key1"));
		keys.add(new String("key7")); //This key is not present.
		//Get a single bin
		System.out.println("Expecting some KEY_NOT_FOUND_ERROR now");
		cr_a = cc.batchGet(params.namespace, params.set, keys, "bin2", null);
		rv = printBatchResult(cr_a, keys, bins);
		if (rv == true) {
			System.out.println("failed 4");
			return false;
		}
		//Get a set of bins
		cr_a = cc.batchGet(params.namespace, params.set, keys, bins, null);
		rv = printBatchResult(cr_a, keys, bins);
		if (rv == true) {
			System.out.println("failed 5");
			return false;
		}
		//Get all the bins
		cr_a = cc.batchGetAll(params.namespace, params.set, keys, null);
		rv = printBatchResult(cr_a, keys, bins);
		if (rv == true) {
			System.out.println("failed 6");
			return false;
		}

		return(true);
	}

	// Test setDigest and getDigest with single String bin name and String value
	public static boolean digestTest1(CitrusleafClient cc, String ns, byte [] digest, Parameters params, String key) {
                ClResultCode rc;
                ClResult cr;
		String binName = "digestBin", binVal = "digestVal", outVal;

		rc = cc.setDigest(ns, digest, binName, binVal, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("setDigest failed for digest - "+digest+", bin - "+binName+", val - "+binVal+". Result - "+rc);
                        return(false);
                }
                cr = cc.get(ns, params.set, key, binName, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("getDigest failed for digest - "+digest+", bin - "+binName+". Result - "+cr.resultCode);
                        return(false);
                } else {
                        //System.out.println(" getDigest got "+cr.results);
                        outVal = (String) cr.results.get(binName);
                        if(outVal.length() != binVal.length()) {
                                System.out.println(" digestTest: Bad Length is "+outVal.length()+" should be "+binVal.length());
                                return(false);
                        }
                        if(outVal.compareTo(binVal) !=0 ) {
                                System.out.println(" digestTest: value is not correct, is "+outVal+" should be "+binVal);
                                return(false);
                        }
                }
                rc = cc.deleteDigest(ns, digest, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("deleteDigest failed for digest - "+digest+". Result - "+rc);
                        return(false);
                }
                cr = cc.getDigest(ns, digest, binName, null);
                if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) {
                        System.out.println("digest - "+digest+" found even after deleteDigest");
                        return(false);
                }
		return(true);
	}

	//Test setDigest with ClBin bin and getAllDigest
	public static boolean digestTest2(CitrusleafClient cc, String ns, byte[] digest) {
                ClResultCode rc;
                ClResult cr;
		ClBin bin = new ClBin("digestBin2","digestVal2");
		String outVal;

		rc = cc.setDigest(ns, digest, bin, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("setDigest failed for digest - "+digest+", bin - "+bin.name+", val - "+bin.value+". Result - "+rc);
                        return(false);
                }
                cr = cc.getAllDigest(ns, digest, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("getDigest failed for digest - "+digest+", bin - "+bin.name+". Result - "+cr.resultCode);
                        return(false);
                } else {
                        //System.out.println(" getDigest got "+cr.results);
                        outVal = (String) cr.results.get(bin.name);
                        if(outVal.compareTo((String)bin.value) !=0 ) {
                                System.out.println(" digestTest: value is not correct, is "+outVal+" should be "+bin.value);
                                return(false);
                        }
                }
                rc = cc.deleteDigest(ns, digest, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("deleteDigest failed for digest - "+digest+". Result - "+rc);
                        return(false);
                }
                cr = cc.getAllDigest(ns, digest, null);
                if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) {
                        System.out.println("digest - "+digest+" found even after deleteDigest");
                        return(false);
                }
		return(true);
	}

	//Test setDigest with HashMap of bins getDigest with ArrayList of bin names
        public static boolean digestTest3(CitrusleafClient cc, String ns, byte[] digest) {
                ClResultCode rc;
                ClResult cr;
		HashMap<String, Object> bins = new HashMap<String, Object>();
                ArrayList<String> binNames = new ArrayList<String>();
		String binName = "digestBin", binVal = "digestVal", addBin, addVal;
		int i, num_bins = 10;

		for (i=0; i<num_bins; i++) {
                        addBin = binName + i;
                        addVal = binVal + i;

                        bins.put(addBin, addVal);
                        binNames.add(addBin);
                }

                rc = cc.setDigest(ns, digest, bins, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("setDigest failed for digest - "+digest+" with HashMap of bins. Result - "+rc);
                        return(false);
                }
                cr = cc.getDigest(ns, digest, binNames, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("getDigest failed for digest - "+digest+" with ArrayList of bins. Result - "+cr.resultCode);
                        return(false);
                } else {
                        //System.out.println(" getDigest got "+cr.results);
                        if (cr.results.size() !=bins.size()) {
                                System.out.println(" digestTest: sizes should be same: is "+cr.results.size()+" should be "+bins.size());
                                return(false);
                        }
                        for (String name: bins.keySet()) {
                                String should_be = (String) bins.get(name);
                                String is = (String) cr.results.get(name);
                                if (should_be.compareTo(is) != 0) {
                                        System.out.println(" key "+name+" should be "+should_be+" is "+is );
                                        return(false);
                                }
                        }
                }
                rc = cc.deleteDigest(ns, digest, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("deleteDigest failed for digest - "+digest+". Result - "+rc);
                        return(false);
                }
		bins.clear();
		binNames.clear();
		return(true);
	}

        //Test setDigest with ArrayList of ClBin bins and getAllDigest
        public static boolean digestTest4(CitrusleafClient cc, String ns, byte[] digest) {
                ClResultCode rc;
                ClResult cr;
		ArrayList <ClBin> bins = new ArrayList<ClBin>();
                String binName = "digestBin", binVal = "digestVal", addBin, addVal;
                int i, num_bins = 10;

                for (i=0; i<num_bins; i++) {
                        addBin = binName + i;
                        addVal = binVal + i;

			bins.add(new ClBin(addBin, addVal));
                }
                rc = cc.setDigest(ns, digest, bins, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("setDigest failed for digest - "+digest+" with ArrayList of bins. Result - "+rc);
                        return(false);
                }
                cr = cc.getAllDigest(ns, digest, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("getDigest failed for digest - "+digest+" with ArrayList of bins. Result - "+cr.resultCode);
                        return(false);
                } else {
                        //System.out.println(" getDigest got "+cr.results);
                        if (cr.results.size() != bins.size()) {
                                System.out.println(" digestTest: sizes should be same: is "+cr.results.size()+" should be "+bins.size());
                                return(false);
                        }
                        for (ClBin bin: bins) {
                                String should_be = (String) bin.value;
                                String is = (String) cr.results.get(bin.name);
                                if (should_be.compareTo(is) != 0) {
                                        System.out.println(" key "+bin.name+" should be "+should_be+" is "+is );
                                        return(false);
                                }
                        }

                }
                rc = cc.deleteDigest(ns, digest, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("deleteDigest failed for digest - "+digest+". Result - "+rc);
                        return(false);
                }
                cr = cc.getAllDigest(ns, digest, null);
                if (cr.resultCode != ClResultCode.KEY_NOT_FOUND_ERROR) {
                        System.out.println("digest - "+digest+" found even after deleteDigest");
                        return(false);
		}
		bins.clear();
		return(true);
	}

	// Method to invoke different digest Tests
	public static boolean digestTest(Parameters params) {
		CitrusleafClient cc = params.cc;
		String ns = params.namespace, key = "digestKey", set = params.set;
		byte[] digest = CitrusleafClient.computeDigest(set,key);
		if (digestTest1(cc, ns, digest, params, key) == false) {
			System.out.println("digestTest1 Failed");
			return(false);
		}
		if (digestTest2(cc,ns, digest) == false) {
			System.out.println("digestTest2 Failed");
                        return(false);
                }
		if (digestTest3(cc,ns, digest) == false) {
                        System.out.println("digestTest3 Failed");
                        return(false);
                }
		if (digestTest4(cc,ns, digest) == false) {
                        System.out.println("digestTest4 Failed");
                        return(false);
                }
		return(true);

	}

	public static boolean nonExistBinTest(Parameters params) {
		
		CitrusleafClient cc = params.cc;
		ClResult result = null;
		ClResultCode rc;
		String ns = params.namespace, key = "someKeyWithBins", set = params.set;

        ArrayList <ClBin> arrayBins = new ArrayList<ClBin> ();

        // initialize the key with some bins
        for (int i=0;i<5;i++) {
        	ClBin bin = new ClBin();
        	bin.name = "testbinname"+i;
        	bin.value = "testbinval"+i;
        	arrayBins.add(bin);
        }
		rc = cc.set(ns, set, key, arrayBins, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("nonExistTest: set failed - "+rc);
			return false;
		}
		
		// read it back using getAll
		result = cc.getAll(ns, set, key, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("nonExistTest: getAll failed - "+rc);
			return false;
		}
		if (result.results.size()!=5) {
			System.out.println("nonExistTest: getAll didn't return 5 bins - instead "+result.results.size());
			return false;
		}
        
        for (int i=0;i<8;i++) {
        	String name = "testbinname"+i;
			result = cc.get(ns, set, key, name, null);
			if (result.resultCode != ClResultCode.OK) {
				System.out.println("nonExistTest: get failed for "+i+ " "+result.resultCode);
				return false;
			}
        }
       
        return true;
	}
	
	// Test to verify that get and set with null bin or values error out properly
	// and with proper values succeed.
	public static boolean nullBinTest(Parameters params) {
		CitrusleafClient cc = params.cc;
		ClResult result = null;
		ClResultCode rc;
		String ns = params.namespace, key = "nullBinKey", set = params.set;

		String val;	
		String nullBin = null;
		String emptyBin = "";
		String myBin = "mybin";
		String myVal = "myvalue";
		String nullVal = null;
		String emptyVal = "";
		ClBin bin = null;
		ArrayList <String> arrayString = null;
        ArrayList <ClBin> arrayBins = null;
		HashMap <String, Object> hashObjects = null;

		rc = cc.set(ns, set, key, nullBin, myVal, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with Null Bin. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		rc = cc.set(ns, set, key, arrayBins, null, null);
		if (rc != ClResultCode.PARAMETER_ERROR) {
			System.out.println("Set with Null ArrayList. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		rc = cc.set(ns, set, key, hashObjects, null, null);
		if (rc != ClResultCode.PARAMETER_ERROR) {
			System.out.println("Set with Null HashMap. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		bin = new ClBin();
        bin.name = nullBin;
        bin.value = myVal;

        arrayBins = new ArrayList<ClBin>(5);
        arrayBins.add(bin);

        rc = cc.set(ns, set, key, arrayBins, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with Null value in ArrayList. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		hashObjects = new HashMap<String, Object>();
		hashObjects.put(nullBin,myVal);

		rc = cc.set(ns, set, key, hashObjects, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with Null bin name in HashMap. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		rc = cc.set(ns, set, key, emptyBin, myVal, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println(" Set with Empty String. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		result = cc.get(ns, set, key, emptyBin, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println(" Get with Empty String. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		val = (String) result.results.get(emptyBin);
		if (! val.equals(myVal)) {
			System.out.println("bin "+myBin+" with value "+myVal+" not found in result. Got "+ val);
			return false;
		}

		result = cc.get(ns, set, key, nullBin, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println(" Get with Null Bin Name. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		result = cc.get(ns, set, key, arrayString, null);
		if (result.resultCode != ClResultCode.PARAMETER_ERROR) {
			System.out.println(" Get with Null Arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		arrayString = new ArrayList<String>(5);
		arrayString.add(nullBin);

		result = cc.get(ns, set, key, arrayString, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println(" Get with Null value in Arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}
		
		rc = cc.delete(ns, set, key, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("delete: should be OK "+rc);
			return(false);
		}

		rc = cc.set(ns, set, key, myBin, myVal, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with Valid String. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}
		result = cc.get(ns, set, key, myBin, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Valid String. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		val = (String) result.results.get(myBin);
		if (! val.equals(myVal)) {
			System.out.println("bin "+myBin+" with value "+myVal+" not found in result. Got "+ val);
			return false;
		}

		rc = cc.delete(ns, set, key, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println(" delete: should be OK "+rc);
			return(false);
		}

		arrayBins.remove(0);
		bin = new ClBin();
        bin.name = myBin;
        bin.value = myVal;
        arrayBins.add(bin);

		bin = new ClBin();
        bin.name = emptyBin;
        bin.value = emptyVal;
        arrayBins.add(bin);
	
		bin = new ClBin();
		bin.name = emptyBin;
		bin.value = nullVal;
		arrayBins.add(bin);

		rc = cc.set(ns, set, key, arrayBins, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with Null value in ArrayList. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		arrayBins.remove(2);
		rc = cc.set(ns, set, key, arrayBins, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with Null value in ArrayList. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		result = cc.get(ns, set, key, arrayString, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Null value in arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}
		
		arrayString.add(myBin);
		arrayString.add(emptyBin);
		result = cc.get(ns, set, key, arrayString, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Null value in arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		result = cc.get(ns, set, key, nullBin, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Null Bin. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		arrayString.remove(0);
		result = cc.get(ns, set, key, arrayString, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with proper values arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}
	
		val = (String) result.results.get(emptyBin);
		if (val.length() != 0) {
			System.out.println("Empty bin and value not found in result");
			return false;
		}

		val = (String) result.results.get(myBin);
		if (! val.equals(myVal)) {
			System.out.println("bin "+myBin+" with value "+myVal+" not found in result. Got "+ val);
			return false;
		}

		result = cc.getAll(ns, set, key, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Null value in arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}
	
		val = (String) result.results.get(emptyBin);
		if (val.length() != 0) {
			System.out.println("Empty bin and value not found in result");
			return false;
		}

		val = (String) result.results.get(myBin);
		if (! val.equals(myVal)) {
			System.out.println("bin "+myBin+" with value "+myVal+" not found in result. Got "+ val);
			return false;
		}
	
		rc = cc.delete(ns, set, key, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println(" delete: should be OK "+rc);
			return(false);
		}

		hashObjects.remove(nullBin);
		hashObjects.put(myBin,nullVal);

		rc = cc.set(ns, set, key, hashObjects, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println(" Set with Null value in HashMap. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		hashObjects.remove(myBin);
		hashObjects.put(myBin,myVal);
		hashObjects.put(emptyBin,emptyVal);
	
		rc = cc.set(ns, set, key, hashObjects, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("Set with proper values in HashMap. Result Code - "+ClResult.resultCodeToString(rc));
			return false;
		}

		result = cc.get(ns, set, key, arrayString, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Null value in arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}
	
		val = (String) result.results.get(emptyBin);
		if (val.length() != 0) {
			System.out.println("Empty bin and value not found in result");
			return false;
		}

		val = (String) result.results.get(myBin);
		if (! val.equals(myVal)) {
			System.out.println("bin "+myBin+" with value "+myVal+" not found in result. Got "+ val);
			return false;
		}
	
		result = cc.getAll(ns, set, key, null);
		if (result.resultCode != ClResultCode.OK) {
			System.out.println("Get with Null value in arraylist. Result Code - "+ClResult.resultCodeToString(result.resultCode));
			return false;
		}

		val = (String) result.results.get(emptyBin);
		if (val.length() != 0) {
			System.out.println("Empty bin and value not found in result");
			return false;
		}

		val = (String) result.results.get(myBin);
		if (! val.equals(myVal)) {
			System.out.println("bin "+myBin+" with value "+myVal+" not found in result. Got "+ val);
			return false;
		}
	
		return true;
	}

	// TUNING PARAMTERS!
	static int g_nthreads = 128;
	static int g_ntasks = 256;

	
	public static class Parameters {
		String  host;
		int		port;
		String namespace;
		String set;
		String bin;
		CitrusleafClient cc;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Options arg_options = new Options();
		arg_options.addOption("h", "host", true, "hostname to query");
		arg_options.addOption("p", "port", true, "port to query [3000]");
		arg_options.addOption("n", "namespace", true, "namespace for all queries");
		arg_options.addOption("k", "key-prefix", true, "prefix for all requests, to allow multiple simultaneous");
		arg_options.addOption("u", "usage", false, "print the usage info");
		arg_options.addOption("s", "set", true, "set to use for this test (default: emptystring");
		arg_options.addOption("b", "bin", true, "bin name to use (default: emptystring)");   
		arg_options.addOption("", "help", false, "get help quick!");
		CommandLineParser clp = new GnuParser();
		
		try {
			CommandLine cl = clp.parse(arg_options, args, false);

			if (cl.hasOption("help") || cl.hasOption("usage")) {
				// Automatically generate the usage information.
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("run_test {<Option>*}", arg_options);
				return;
			}
			runTests(cl);
		}	
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
			e.printStackTrace();
			return;
		}
	}
	
	public static void runTests(CommandLine cl) {
		Parameters params = new Parameters();
		params.host = cl.getOptionValue("h", "127.0.0.1");
		String port_s = cl.getOptionValue("p", "3000");
		params.port = Integer.valueOf(port_s);
		params.namespace = cl.getOptionValue("n","test");
		params.bin = cl.getOptionValue("b","");
		params.set = cl.getOptionValue("s", "");
		
		System.out.println(" Java functionality tests: cluster "+params.host+":"+params.port+" ns: "+
				params.namespace+" bin: "+params.bin+" set: "+params.set);

		// Connect to the configured cluster
		params.cc = new CitrusleafClient(params.host, params.port);
		if (params.cc == null) {
			System.out.println(" Cluster "+params.host+":"+params.port+" could not be contacted");
			return;
		}
			
		if (nullBinTest(params) == true) {
			System.out.println(" null bin tests succeeded");
		} else {
			System.out.println(" null bin tests failed");
			return;
		}
	
		if (nonExistBinTest(params) == true ) {
			System.out.println(" nonExist bin test succeeded");
		} else {
			System.out.println(" nonExist bin test failed");
			return;
		}

		if (setAndExpireTest(params )) {
			System.out.println(" setAndExpire test succeeded");
		}
		else {
			System.out.println(" setAndExpire test failed");
			return;
		}

		if (getWithTouchTest(params ) == true) {
			System.out.println(" getWithTouch test succeeded");
		}
		else {
			System.out.println(" getWithTouch test failed");
			return;
		}

		if (simplePrependTest(params ) == true) {
			System.out.println(" simpleprepend test succeeded");
		}
		else {
			System.out.println(" simplePrepend test failed");
			return;
		}



		if (simpleAppendTest(params ) == true) {
			System.out.println(" simpleAppend test succeeded");
		}
		else {
			System.out.println(" simpleAppend test failed");
			return;
		}


		if (xplusoneAppendTest(params) == true) {
			System.out.println(" xplusoneAppendTest succeeded");
		}else{
			System.out.println(" xplusoneAppendTest failed");
			return;
		}

		if (batchTest(params) == true) {
			System.out.println(" batch test suceeded ");
		} else {
			System.out.println(" batch test failed");
			return;
		}

		if (AddTest.execute(params) == true) {
			System.out.println(" add test suceeded ");
		}
		else {
			System.out.println(" add test failed");
			return;
		}

		// run each test
		if (multiBinTest(params ) == true) {
			System.out.println(" multibin test succeeded");
		}
		else {
			System.out.println(" multibin test failed");
			return;
		}

		if (deleteTest(params ) == true) {
			System.out.println(" delete test succeeded");
		}
		else {
			System.out.println(" delete test failed");
			return;
		}

		if (generationTest(params ) == true) {
			System.out.println(" generation test succeeded");
		}
		else {
			System.out.println(" generation test failed");
			return;
		}

  		if (genDuplicateTest(params ) == true) {
			System.out.println(" duplicate generation test succeeded");
		}
		else {
			System.out.println(" duplicate generation test failed");
			return;
		}

		if (blobTest(params ) == true) {
			System.out.println(" blob test succeeded");
		}
		else {
			System.out.println(" blob test failed");
			return;
		}

		if (largeBlobTest(params ) == true) {
			System.out.println(" large blob test succeeded");
		}
		else {
			System.out.println(" large blob test failed");
			// Keep going...
			//return;
		}
		if (largeBlobMultiTest(params ) == true) {
			System.out.println(" large blob test succeeded");
		}
		else {
			System.out.println(" large blob test failed");
			return;
		}

		if (nullKeyTest(params) == true) {
			System.out.println(" nullkey test succeeded");
		}
		else {
			System.out.println(" nullkey test failed");
			return;
		}

		if (largeKeyTest(params ) == true) {
			System.out.println(" largeKey test succeeded");
		}
		else {
			System.out.println(" largeKey test failed");
			return;
		}

		if (intTest(params ) == true) {
			System.out.println(" int test succeeded");
		}
		else {
			System.out.println(" int test failed");
			return;
		}
		
		if (negIntTest(params) == true) {
			System.out.println(" negIntTest succeeded");
		}
		else {
			System.out.println(" negIntTest failed");
			return;
		}
		
		if (uniqueInsertTest(params) == true) {
			System.out.println(" uniqueInsert succeeded");
		}
		else {
			System.out.println(" uniqueInsert failed");
			return;
		}
		
		if (serializeTest(params ) == true) {
			System.out.println(" serialize test (10K integers) succeeded");
		}
		else {
			System.out.println(" serialize test failed");
			return;
		}

		if (ScanTest.execute(params) == true) {
			System.out.println(" Scan test succeeded");
		}
		else {
			System.out.println(" Scan test failed");
			return;
		} 

		if (digestTest(params) == true) {
			System.out.println(" digest tests succeeded");
		} else {
			System.out.println(" digest tests failed");
			return;
		}
	}
}
