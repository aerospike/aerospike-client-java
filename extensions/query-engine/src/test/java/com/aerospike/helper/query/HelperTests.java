package com.aerospike.helper.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//@RunWith(Parameterized.class)
public abstract class HelperTests extends AerospikeAwareTests {

	protected static final String BLUE = "blue";
	protected static final String ORANGE = "orange";
	protected static final String GREEN = "green";
	protected static final String RED = "red";
	protected static final String YELLOW = "yellow";

	// These values are added to Lists and Maps to avoid single item collections.
	// Tests should ignore them for assertion purposes
	protected static final long skipLongValue = Long.MAX_VALUE;
	protected static final String skipColorValue = "SKIP_THIS_COLOR";

	protected int[] ages = new int[]{25,26,27,28,29};
	protected String[] colours = new String[]{BLUE, RED, YELLOW, GREEN, ORANGE};
	protected String[] animals = new String[]{"cat","dog","mouse","snake","lion"};

	protected String geoSet = "geo-set", geoBinName = "querygeobin";
	protected String specialCharBin = "scBin";
	private static final String keyPrefix = "querykey";

	protected Map<Integer, Integer> recordsWithAgeCounts;
	protected Map<String, Integer> recordsWithColourCounts;
	protected Map<String, Integer> recordsWithAnimalCounts;
	protected Map<Long, Integer> recordsModTenCounts;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		int i = 0;
		Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ 10);

		initializeMaps();
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			Map<Long, String>ageColorMap = new HashMap<>();
			ageColorMap.put((long) ages[i], colours[i]);
			ageColorMap.put(skipLongValue, skipColorValue);

			Map<String, Long>colorAgeMap = new HashMap<>();
			colorAgeMap.put(colours[i], (long) ages[i]);
			colorAgeMap.put(skipColorValue, skipLongValue);

			List<String>colorList = new ArrayList<>();
			colorList.add(colours[i]);
			colorList.add(skipColorValue);

			List<Long>longList = new ArrayList<>();
			longList.add((long)ages[i]);
			longList.add(skipLongValue);

			key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ x);
			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			Bin modTen = new Bin("modten", i % 10);

			Bin ageColorMapBin = new Bin("ageColorMap", ageColorMap);
			Bin colorAgeMapBin = new Bin("colorAgeMap", colorAgeMap);
			Bin colorListBin = new Bin("colorList", colorList);
			Bin longListBin = new Bin("longList", longList);

			this.client.put(null, key, name, age, colour, animal, modTen, ageColorMapBin,
					colorAgeMapBin, colorListBin, longListBin);
			// Add to our counts of records written for each bin value
			recordsWithAgeCounts.put(ages[i], recordsWithAgeCounts.get(ages[i]) + 1);
			recordsWithColourCounts.put(colours[i], recordsWithColourCounts.get(colours[i]) + 1);
			recordsWithAnimalCounts.put(animals[i], recordsWithAnimalCounts.get(animals[i]) + 1);
			recordsModTenCounts.put((long) (i % 10), recordsModTenCounts.get((long)(i % 10)) + 1);

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
		ptsb.append(lg);
		ptsb.append(", ");
		ptsb.append(lat);
		ptsb.append("] }");
		return ptsb.toString();
	}

	private void initializeMaps() {
		recordsWithAgeCounts = new HashMap<>();
		for (int age: ages) {
			recordsWithAgeCounts.put(age, 0);
		}
		recordsWithColourCounts = new HashMap<>();
		for (String colour: colours) {
			recordsWithColourCounts.put(colour, 0);
		}
		recordsWithAnimalCounts = new HashMap<>();
		for (String animal: animals) {
			recordsWithAnimalCounts.put(animal, 0);
		}
		recordsModTenCounts = new HashMap<>();
		for (long i = 0; i < 10; i++) {
			recordsModTenCounts.put(i, 0);
		}
	}
	
	protected void tryDropIndex(String namespace, String setName, String indexName) {
		try {
			wait(client.dropIndex(null, namespace, setName, indexName));
		}  catch (AerospikeException e) {
			if (e.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw e;
			}
		}
		queryEngine.refreshCluster();

	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType) {
		try {
			wait(this.client.createIndex(null, namespace, setName, indexName, binName, indexType));
		} catch (AerospikeException e) {
			if (e.getResultCode() != ResultCode.INDEX_FOUND) {
				throw(e);
			}
		}
		queryEngine.refreshCluster();

	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType, 
			IndexCollectionType collectionType) {
		try {
			wait(this.client.createIndex(null, namespace, setName, indexName, binName, indexType, collectionType));
		} catch (AerospikeException e) {
			if (e.getResultCode() != ResultCode.INDEX_FOUND) {
				throw(e);
			}
		}
		queryEngine.refreshCluster();
	}
}
