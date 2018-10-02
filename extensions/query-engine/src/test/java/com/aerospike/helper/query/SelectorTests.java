package com.aerospike.helper.query;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.helper.query.GenerationQualifier;
import com.aerospike.helper.query.KeyQualifier;
import com.aerospike.helper.query.KeyRecordIterator;
import com.aerospike.helper.query.Qualifier;

public class SelectorTests extends HelperTests{



	public SelectorTests() {
		super();
	}

	@Test
	public void selectOneWitKey() throws IOException {
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(TestQueryEngine.SET_NAME);
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));
		KeyRecordIterator it = queryEngine.select(stmt, kq);
		int count = 0;
		while (it.hasNext()){
			it.next();
			count++;
		}
		it.close();
		Assert.assertEquals(1, count);
	}

	@Test
	public void selectAll() throws IOException {
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
		try {
			int count = 0;
			while (it.hasNext()){
				it.next();
				count++;
			}
			Assert.assertEquals(1000, count);
		} finally {
			it.close();
		}
	}

	@Test
	public void selectOnIndex() throws IOException {
		IndexTask task = this.client.createIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete(50);
		Filter filter = Filter.range("age", 28, 29);
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				Assert.assertTrue(age >= 28 && age <= 29);
			}
		} finally {
			this.client.dropIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
			it.close();
		}
	}
	@Test
	public void selectStartsWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertTrue(rec.record.getString("color").endsWith("e"));
			}
		} finally {
			it.close();
		}
	}
	@Test
	public void selectEndsWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertEquals("blue", rec.record.getString("color"));
				Assert.assertTrue(rec.record.getString("name").startsWith("na"));
			}
		} finally {
			it.close();
		}
	}
	@Test
	public void selectOnIndexWithQualifiers() throws IOException {
		IndexTask task = this.client.createIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector", "age", IndexType.NUMERIC);
		task.waitTillComplete(50);
		Filter filter = Filter.range("age", 25, 29);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qual1);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertEquals("blue", rec.record.getString("color"));
				int age = rec.record.getInt("age");
				Assert.assertTrue(age >= 25 && age <= 29);
			}
		} finally {
			this.client.dropIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector");
			it.close();
		}
	}
	@Test
	public void selectWithQualifiersOnly() throws IOException {
		IndexTask task = this.client.createIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete(50);
		queryEngine.refreshCluster();
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertEquals("green", rec.record.getString("color"));
				int age = rec.record.getInt("age");
				Assert.assertTrue(age >= 28 && age <= 29);
			}
		} finally {
			this.client.dropIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
			it.close();
		}
	}

	@Test
	public void selectWithOrQualifiers() throws IOException {
		IndexTask task = this.client.createIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete(50);
		queryEngine.refreshCluster();
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual2);
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, or);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				Assert.assertTrue("green"==rec.record.getString("color") ||(age >= 28 && age <= 29));
			}
		} finally {
			this.client.dropIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
			it.close();
		}
	}

	@Test
	public void selectWithBetweenAndOrQualifiers() throws IOException {
		IndexTask task = this.client.createIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete(50);
		queryEngine.refreshCluster();
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		Qualifier qual3 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Qualifier qual4 = new Qualifier("name", Qualifier.FilterOperation.EQ, Value.get("name:696"));
		Qualifier or = new Qualifier(Qualifier.FilterOperation.OR, qual3, qual2, qual4);
		Qualifier or2 = new Qualifier(Qualifier.FilterOperation.OR, qual1, qual4);
		Qualifier and = new Qualifier(Qualifier.FilterOperation.AND, or, or2);
		
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, and);
		try{
			boolean has25=false;
			while (it.hasNext()){
				KeyRecord rec = it.next();
				int age = rec.record.getInt("age");
				if(age==25) has25=true;
				else Assert.assertTrue("green".equals(rec.record.getString("color")) && (age == 25 || (age>=28 && age<=29)));
			}
			Assert.assertTrue(has25);
		} finally {
			this.client.dropIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index");
			it.close();
		}
	}

	@Test
	public void selectWithGeneration() throws IOException {
		queryEngine.refreshCluster();
		Qualifier qual1 = new GenerationQualifier(Qualifier.FilterOperation.GTEQ, Value.get(1));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		try {
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertTrue(rec.record.generation >= 1);
			}
		} finally {
			it.close();
		}
	}

	@Test
	public void selectWithGeoWithin() throws IOException{
		double lon= -122.0;
		double lat= 37.5;
		double radius=50000.0;
		String rgnstr = String.format("{ \"type\": \"AeroCircle\", "
							  + "\"coordinates\": [[%.8f, %.8f], %f] }",
							  lon, lat, radius);
		Statement stmt = new Statement();
		stmt.setNamespace("test");
		stmt.setSetName(geoSet);
		Qualifier qual1 = new Qualifier(geoSet, Qualifier.FilterOperation.GEO_WITHIN, Value.getAsGeoJSON(rgnstr));
		KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
		try {
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertTrue(rec.record.generation >= 1);
			}
		} finally {
			it.close();
		}
	}
}
