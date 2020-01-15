package com.aerospike.helper.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.Statement;

public class UpdatorTests extends HelperTests{

	@Test
	public void updateByKey(){
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			String keyString = "selector-test:"+x;
			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
			KeyQualifier kq = new KeyQualifier(Value.get(keyString));
			Statement stmt = new Statement();
			stmt.setNamespace(TestQueryEngine.NAMESPACE);
			stmt.setSetName(TestQueryEngine.SET_NAME);
			List<Bin> bins = Arrays.asList(new Bin("ending", "ends with e"));

			Map<String, Long> counts = queryEngine.update(stmt, bins, kq );

			Assert.assertEquals((Long)1L, (Long)counts.get("write"));
			Record record = this.client.get(null, key);
			Assert.assertNotNull(record);
			String ending = record.getString("ending");
			Assert.assertTrue(ending.endsWith("ends with e"));
		}
	}
	@Test
	public void updateByDigest(){

			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
				String keyString = "selector-test:"+x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(key.digest);
				Statement stmt = new Statement();
				stmt.setNamespace(TestQueryEngine.NAMESPACE);
				stmt.setSetName(TestQueryEngine.SET_NAME);
				List<Bin> bins = Arrays.asList(new Bin("ending", "ends with e"));

				Map<String, Long> counts = queryEngine.update(stmt, bins, kq );

				Assert.assertEquals((Long)1L, (Long)counts.get("write"));
				Record record = this.client.get(null, key);
				Assert.assertNotNull(record);
				String ending = record.getString("ending");
				Assert.assertTrue(ending.endsWith("ends with e"));
			}
	}
	@Test
	public void updateStartsWith() {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		List<Bin> bins = Arrays.asList(new Bin("ending", "ends with e"));
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(TestQueryEngine.SET_NAME);

		Map<String, Long> counts = queryEngine.update(stmt, bins, qual1);

		Assert.assertEquals((Long)400L, (Long)counts.get("read"));
		Assert.assertEquals((Long)400L, (Long)counts.get("write"));
	}

	@Test
	public void updateEndsWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
		List<Bin> bins = Arrays.asList(new Bin("starting", "ends with e"));
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(TestQueryEngine.SET_NAME);

		Map<String, Long> counts = queryEngine.update(stmt, bins, qual1, qual2);

		Assert.assertEquals((Long)200L, (Long)counts.get("read"));
		Assert.assertEquals((Long)200L, (Long)counts.get("write"));
	}

}
