package com.aerospike.helper.query;

import org.junit.After;
import org.junit.Before;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

//@RunWith(Parameterized.class)
public abstract class HelperTests extends AerospikeAwareTests {

	protected int[] ages = new int[]{25,26,27,28,29};
	protected String[] colours = new String[]{"blue","red","yellow","green","orange"};
	protected String[] animals = new String[]{"cat","dog","mouse","snake","lion"};

	protected String geoSet = "geo-set", geoBinName = "querygeobin";

	private static final String keyPrefix = "querykey";

	@Before
	public void setUp() throws Exception {
		super.setUp();
		int i = 0;
		Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ 10);
		if (this.client.exists(null, key))
			return;
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ x);
			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			this.client.put(null, key, name, age, colour, animal);
			i++;
			if ( i == 5)
				i = 0;
		}
		
		//GEO Test setup
		for (i = 0; i < TestQueryEngine.RECORD_COUNT; i++) {
			double lng = -122 + (0.1 * i);
			double lat = 37.5 + (0.1 * i);
			key = new Key(TestQueryEngine.NAMESPACE, geoSet, keyPrefix + i);
			Bin bin = Bin.asGeoJSON(geoBinName, buildGeoValue(lng, lat));
			client.put(null, key, bin);
		}
	}

	@After
	public void tearDown() throws Exception {
		client.truncate(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
		client.truncate(null, TestQueryEngine.NAMESPACE, geoSet, null);
		super.tearDown();
	}

	private static String buildGeoValue(double lg, double lat) {
		StringBuilder ptsb = new StringBuilder();
		ptsb.append("{ \"type\": \"Point\", \"coordinates\": [");
		ptsb.append(String.valueOf(lg));
		ptsb.append(", ");
		ptsb.append(String.valueOf(lat));
		ptsb.append("] }");
		return ptsb.toString();
	}
}
