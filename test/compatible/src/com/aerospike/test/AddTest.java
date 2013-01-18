/*
 *  Copyright 2012 by Aerospike, Inc.  All rights reserved.
 *  
 *  Availability of this source code to partners and customers includes
 *  redistribution rights covered by individual contract. Please check
 *  your contract for exact rights and responsibilities.
 */
package com.aerospike.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import net.citrusleaf.CitrusleafClient;
import net.citrusleaf.CitrusleafClient.ClBin;
import net.citrusleaf.CitrusleafClient.ClResult;
import net.citrusleaf.CitrusleafClient.ClResultCode;


public class AddTest {
    
    // this is a more basic test with a lot of flushing in between
    
    public static boolean test1(Main.Parameters params) {

        CitrusleafClient cc = params.cc;
        ClResultCode rc;
        ClResult cr;
        Vector<ClBin> vv;
        
        String ns = params.namespace;
        String set = params.set;
        String addKey = "myaddkey";
        String bin = "addbin";

        // Delete prior state so this test can be run repeatedly
        // don't pay attention to the return
        cc.delete(ns, set, addKey, null, null);
        
 		vv = new Vector<ClBin>();
		vv.add(new ClBin(bin, 7));
		
		rc = cc.set(ns, set, addKey, vv, null, null);		
		if (rc != ClResultCode.OK) {
			System.out.println("couldn't do initial set "+rc);
			return(false);		
		}
		
		vv = new Vector<ClBin>();
		vv.add(new ClBin(bin, 4));

		// add one
		rc = cc.add(ns, set, addKey, vv, null, null);		
		if (rc != ClResultCode.OK) {
			System.out.println("couldn't do add "+rc);
			return(false);		
		}
		
		// get the key
		cr = cc.get(ns, set, addKey, bin, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println("couldn't do get "+rc);
			return(false);		
		} else {
			Object result = cr.result;
			try {
				int intResult = (Integer) result;
				if(intResult != 11) {
					System.out.println("add value should be 11, but is "+intResult);
                    return (false);
				}
			} catch (Exception e) {
				System.out.println("couldn't convert result to int "+e.toString());
                return (false);
			}
		}
		
		return(true);
  }
  
	public static boolean test2(Main.Parameters params) {

		CitrusleafClient cc = params.cc;
		ClResultCode rc;
		ClResult cr;
		Vector<ClBin> vv;

		String ns = params.namespace;
		String set = params.set;
		String addKey = "myaddkey";
		String bin1 = "addbin1";
		String bin2 = "addbin2";

		// Delete prior state so this test can be run repeatedly
		// don't pay attention to the return
		cc.delete(ns, set, addKey, null, null);

		vv = new Vector<ClBin>();
		vv.add(new ClBin(bin1, 7));
		vv.add(new ClBin(bin2, 9));

		rc = cc.set(ns, set, addKey, vv, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("couldn't do initial set "+rc);
			return(false);
		}

		vv = new Vector<ClBin>();
		vv.add(new ClBin(bin1, 4));
		vv.add(new ClBin(bin2, 9));

		// add value to existing key and get back the key
		cr = cc.addAndGet(ns, set, addKey, vv, null, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println("couldn't do get "+rc);
			return(false);
		} else {
			try {
				int r1 = (Integer) cr.results.get(bin1);
				int r2 = (Integer) cr.results.get(bin2);
				if((r1 != 11) || (r2 != 18)) {
					System.out.println("add value should be 11 & 18, but is "+r1+" & "+r2);
					return (false);
				}
			} catch (Exception e) {
				System.out.println("couldn't convert result to int "+e.toString());
				return (false);
			}
		}

		return(true);
	}

	//Same as test2 but using hash map instead of vector
	public static boolean test3(Main.Parameters params) {
		CitrusleafClient cc = params.cc;
		ClResultCode rc;
		ClResult cr;
		Vector<ClBin> vv;

		String ns = params.namespace;
		String set = params.set;
		String addKey = "myaddkey";
		String bin1 = "addbin1";
		String bin2 = "addbin2";

		// Delete prior state so this test can be run repeatedly
		// don't pay attention to the return
		cc.delete(ns, set, addKey, null, null);

		vv = new Vector<ClBin>();
		vv.add(new ClBin(bin1, 7));
		vv.add(new ClBin(bin2, 9));

		rc = cc.set(ns, set, addKey, vv, null, null);
		if (rc != ClResultCode.OK) {
			System.out.println("couldn't do initial set "+rc);
			return(false);
		}

		HashMap<String, Object> vals = new HashMap<String, Object>();
		vals.put("addbin1", 4);
		vals.put("addbin2", 9);

		// add value to existing key and get back the key
		cr = cc.addAndGet(ns, set, addKey, vals, null, null);
		if (cr.resultCode != ClResultCode.OK) {
			System.out.println("couldn't do get "+rc);
			return(false);
		} else {
			try {
				int r1 = (Integer) cr.results.get(bin1);
				int r2 = (Integer) cr.results.get(bin2);
				if((r1 != 11) || (r2 != 18)) {
					System.out.println("add value should be 11 & 18, but is "+r1+" & "+r2);
					return (false);
				}
			} catch (Exception e) {
				System.out.println("couldn't convert result to int "+e.toString());
				return (false);
			}
		}

		return(true);
	}

// Test for addDigest with a single ClBin bin
    public static boolean test4(Main.Parameters params) {
        CitrusleafClient cc = params.cc;
        ClResultCode rc;
        ClResult cr;

        String ns = params.namespace;
        String set = params.set;
        String addKey = "myaddkey";
        String bin = "addbin";
        byte[] digest = CitrusleafClient.computeDigest(set, addKey);

        // Delete prior state so this test can be run repeatedly
        // don't pay attention to the return
        cc.deleteDigest(ns, digest, null, null);

                ClBin clbin = new ClBin(bin,7);

                rc = cc.setDigest(ns, digest, clbin, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do initial set "+rc);
                        return(false);
                }
                clbin.value = 4;

                // add one
                rc = cc.addDigest(ns, digest, clbin, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do add "+rc);
                        return(false);
                }

	                // get the key
                cr = cc.getDigest(ns, digest, bin, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("couldn't do get "+rc);
                        return(false);
                } else {
                        try {
                                int intResult = (Integer) cr.results.get(bin);
                                if(intResult != 11) {
                                        System.out.println("add value should be 11, but is "+intResult);
                    return (false);
                                }
                        } catch (Exception e) {
                                System.out.println("2 couldn't convert result to int "+e.toString());
                return (false);
                        }
                }

                return(true);
  }

// Test for addDigest with a single binname and value
    public static boolean test5(Main.Parameters params) {
        CitrusleafClient cc = params.cc;
        ClResultCode rc;
        ClResult cr;

        String ns = params.namespace;
        String set = params.set;
        String addKey = "myaddkey";
        String bin = "addbin";
        byte[] digest = CitrusleafClient.computeDigest(set, addKey);

        // Delete prior state so this test can be run repeatedly
        // don't pay attention to the return
        cc.deleteDigest(ns, digest, null, null);

                rc = cc.setDigest(ns, digest, bin, 7, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do initial set "+rc);
                        return(false);
                }
                // add one
                rc = cc.addDigest(ns, digest, bin, 4, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do add "+rc);
                        return(false);
                }

                        // get the key
                cr = cc.getDigest(ns, digest, bin, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("couldn't do get "+rc);
                        return(false);
                } else {
                        try {
                                int intResult = (Integer) cr.results.get(bin);
                                if(intResult != 11) {
                                        System.out.println("add value should be 11, but is "+intResult);
                    return (false);
                                }
                        } catch (Exception e) {
                                System.out.println("2 couldn't convert result to int "+e.toString());
                return (false);
                        }
                }

                return(true);
  }

// Test for addDigest with a ArrayList of bins
    public static boolean test6(Main.Parameters params) {
        CitrusleafClient cc = params.cc;
        ClResultCode rc;
        ClResult cr;

        String ns = params.namespace;
        String set = params.set;
        String addKey = "myaddkey", binName = "addBin", addBin;
        ArrayList <ClBin> bins = new ArrayList<ClBin>();
        byte[] digest = CitrusleafClient.computeDigest(set, addKey);
        int i, addVal, num_bins = 10;

        // Delete prior state so this test can be run repeatedly
        // don't pay attention to the return
        cc.deleteDigest(ns, digest, null, null);

                for (i=0; i<num_bins; i++) {
                        addBin = binName + i;
                        addVal = i;

			bins.add(new ClBin(addBin, addVal));
                }

                rc = cc.setDigest(ns, digest, bins, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do initial set "+rc);
                        return(false);
                }
                // add one
                rc = cc.addDigest(ns, digest, bins, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do add "+rc);
                        return(false);
                }

                        // get the key
                cr = cc.getAllDigest(ns, digest, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("couldn't do get "+rc);
                        return(false);
                } else {
                        try {
				for (ClBin bin: bins) {
                                        Integer should_be = (Integer) bin.value * 2;
                                        Integer is = (Integer) cr.results.get(bin.name);
					if (should_be.compareTo(is) != 0) {
						System.out.println(" key "+bin.name+" should be "+should_be+" is "+is );
						return(false);
					}
				}
                        } catch (Exception e) {
                                System.out.println("2 couldn't convert result to int "+e.toString());
                                return (false);
                        }
                }

                return(true);
  }


// Test for addDigest with a HashMap of bins
    public static boolean test7(Main.Parameters params) {
        CitrusleafClient cc = params.cc;
        ClResultCode rc;
        ClResult cr;

        String ns = params.namespace;
        String set = params.set;
        String addKey = "myaddkey", binName = "addBin", addBin;
        HashMap<String, Object> bins = new HashMap<String, Object>();
		ArrayList<String> binNames = new ArrayList<String>();
        byte[] digest = CitrusleafClient.computeDigest(set, addKey);
        int i, addVal, num_bins = 10;

        // Delete prior state so this test can be run repeatedly
        // don't pay attention to the return
        cc.deleteDigest(ns, digest, null, null);

                for (i=0; i<num_bins; i++) {
                        addBin = binName + i;
                        addVal = i;

			bins.put(addBin, addVal);
                        binNames.add(addBin);
                }

                rc = cc.setDigest(ns, digest, bins, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do initial set "+rc);
                        return(false);
                }
                // add one
                rc = cc.addDigest(ns, digest, bins, null, null);
                if (rc != ClResultCode.OK) {
                        System.out.println("couldn't do add "+rc);
                        return(false);
                }

                        // get the key
                cr = cc.getDigest(ns, digest, binNames, null);
                if (cr.resultCode != ClResultCode.OK) {
                        System.out.println("couldn't do get "+rc);
                        return(false);
                } else {
                        try {
                                for (String name: bins.keySet()) {
                                        Integer should_be = (Integer) bins.get(name) * 2;
                                        Integer is = (Integer) cr.results.get(name);
                                        if (should_be.compareTo(is) != 0) {
                                                System.out.println(" key "+name+" should be "+should_be+" is "+is );
                                                return(false);
                                        }
                                }
                        } catch (Exception e) {
                                System.out.println("2 couldn't convert result to int "+e.toString());
                                return (false);
                        }
                }

                return(true);
  }

    public static 
    boolean execute(Main.Parameters params) {

		System.out.println("Citrusleaf Host: "+params.host + " Port: "+ params.port);
		
		if (false == test1(params)) {
			System.out.println("add test 1 failed");
			return(false);
		} else if (false == test2(params)) {
			System.out.println("add test 2 failed");
			return(false);
		} else if (false == test3(params)) {
			System.out.println("add test 3 failed");
			return(false);
		} else if (false == test4(params)) {
			System.out.println("add test 4 failed");
			return(false);
		} else if (false == test5(params)) {
                        System.out.println("add test 5 failed");
                        return(false);
		} else if (false == test6(params)) {
                        System.out.println("add test 6 failed");
                        return(false);
		} else if (false == test7(params)) {
                        System.out.println("add test 7 failed");
                        return(false);
		} else {
			System.out.println("add test succeeded");
		}
		return(true);		
	}
}
    
    

