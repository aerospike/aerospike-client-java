package com.aerospike.helper.query;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.KeyQualifier;
import com.aerospike.helper.query.QueryEngine;

public class InserterTests extends HelperTests{

	public InserterTests() {
		super();
	}
	@Before
	public void setUp() {
//		if (this.useAuth){
//			clientPolicy = new ClientPolicy();
//			clientPolicy.failIfNotConnected = true;
//			clientPolicy.user = QueryEngineTests.AUTH_UID;
//			clientPolicy.password = QueryEngineTests.AUTH_PWD;
//			client = new AerospikeClient(clientPolicy, QueryEngineTests.AUTH_HOST, QueryEngineTests.AUTH_PORT);
//		} else {
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
//		}
		queryEngine = new QueryEngine(client);

	}

	@After
	public void tearDown() throws Exception {
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ x);
			this.client.delete(null, key);
		}
		queryEngine.close();
	}

	@Test
	public void insertByKey(){
		int i = 0;
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			String keyString = "selector-test:"+x;

			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			List<Bin> bins = Arrays.asList(name, age, colour, animal);

			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
			this.client.delete(null, key);

			KeyQualifier kq = new KeyQualifier(Value.get(keyString));
			Statement stmt = new Statement();
			stmt.setNamespace(TestQueryEngine.NAMESPACE);
			stmt.setSetName(TestQueryEngine.SET_NAME);

			queryEngine.insert(stmt, kq, bins);

			Record record = this.client.get(null, key);
			Assert.assertNotNull(record);
			i++;
			if ( i == 5)
				i = 0;
		}
	}
	@Test
	public void insertByDigest(){

		int i = 0;
		for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
			String keyString = "selector-test:"+x;

			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			List<Bin> bins = Arrays.asList(name, age, colour, animal);

			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
			this.client.delete(null, key);

			KeyQualifier kq = new KeyQualifier(key.digest);
			Statement stmt = new Statement();
			stmt.setNamespace(TestQueryEngine.NAMESPACE);
			stmt.setSetName(TestQueryEngine.SET_NAME);

			queryEngine.insert(stmt, kq, bins);

			Record record = this.client.get(null, key);
			Assert.assertNotNull(record);
			i++;
			if ( i == 5)
				i = 0;
		}
	}

}
