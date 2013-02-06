package com.aerospike.benchmarks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;

public class Utils {
	protected static Bin[] genBins(Random r, int binSize, DBObjectSpec[] spec, int generation) {
		Bin[] bins = new Bin[binSize];
		for(int i=0; i<binSize; i++) {
			String name = Integer.toString(i);
			Value value = genValue(r, spec[i%spec.length].type, spec[i%spec.length].size, generation);
			bins[i] = new Bin(name, value);
		}
		return bins;
	}

   protected static Value genValue(Random r, char type, int size, int generation) {
		if(type == 'B') {
			byte[] ba = new byte[size];
			r.nextBytes(ba);
			return Value.get(ba);
		} else if(type == 'D') {
			return Value.get(Integer.toString((int) (new Date().getTime()%86400000))+","+Integer.toString(generation));
		} else {
			int v = r.nextInt();
			v = v < 0 ? (-v) : v;
			if(type == 'I') {
				return Value.get(v);
			} else if(type == 'S') {
				String vs_sm = Integer.toString(v);
				String vs = "";
				while(vs.length() < size) {
					vs += vs_sm;
				}
				return Value.get(vs.substring(vs.length()-size));
			}
		}
		return Value.getAsNull();
	}
	
	protected static String genKey(int i, int keyLen) {
		String key = "";
		for(int j=keyLen-1; j>=0; j--) {
			key = (i % 10) + key;
			i /= 10;
		}
		return key;
	}
	
	protected static void writeMismatchedKVP(int client_num, String clientdir, String key, Object getVal, Object toVal) {
		File f = new File(clientdir+"mismatchedKVP_"+client_num+".csv");
		if(!f.exists()) {
			try {
				f.createNewFile();
			} catch (Exception e) {
				System.out.println("couldn't create new file for some reason");
			}
		}
		try {
			FileWriter fw = new FileWriter(clientdir+"mismatchedKVP_"+client_num+".csv", true);
			BufferedWriter out = new BufferedWriter(fw);
			out.write("key: "+key+", expected:"+getVal+", found:"+toVal+"\n");
			out.close();
		} catch (Exception e) {
			System.out.println("couldn't write to file for some reason");
		}

	}
}
