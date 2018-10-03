# QueryEngine

The Query engine class uses an instance of an `AerospikeClient` to implement operations familiar to most developers. A QueryEngine instance contain no state and is thread safe. 

Each operation of 
 - Select
 - Insert
 - Update
 - Delete

Zero or More filters (predicates) can be specified for each operation. Filtering is done in server in the Aerospike cluster.

##Creating a query engine
Creating a QueryEngine is quite simple, the constructor is passed an instance of AerospikeClient.

```java
	AerospikeClient client = new AerospikeClient(clientPolicy, "127.0.0.1", 3000);
	
	. . .
	
	QueryEngine queryEngine = new QueryEngine(client);

```


## Select example

Records are selected using a list of the Criteria objects.  In this example you can see the equivalent of this sql statement:
```sql
	SELECT * FROM test.selector WHERE color = 'blue' AND name LIKE 'na'
```
Java
```java
	Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
	Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
	KeyRecordIterator it = queryEngine.select("test", "selector", null, qual1, qual2);
	try{
		while (it.hasNext()){
			KeyRecord rec = it.next();
			Assert.assertEquals("blue", rec.record.getString("color"));
			Assert.assertTrue(rec.record.getString("name").startsWith("na"));
		}
	} finally {
		it.close();
	}
```
## Insert example
This example is an insert equivalent to this sql statement:
```sql
	INSERT INTO test.selector (PK,name,age,color,animal) 
	VALUES (keyString,value1,value2,value3,value4)
```
Java

```java
	Bin name = new Bin("name", "name:" + x);
	Bin age = new Bin("age", ages[i]);
	Bin colour = new Bin("color", colours[i]);
	Bin animal = new Bin("animal", animals[i]);
	List<Bin> bins = Arrays.asList(name, age, colour, animal);
			
	Key key = new Key("test", "selector", keyString);
	KeyQualifier kq = new KeyQualifier(key.digest);
	Statement stmt = new Statement();
	stmt.setNamespace(QueryEngineTests.NAMESPACE);
	stmt.setSetName(QueryEngineTests.SET_NAME);
			
	queryEngine.insert(stmt, kq, bins);

```
## Update example
Here is a update example that uses a filter, similar to this SQL:
```sql
	UPDATE test.selector
	SET ending ='ends with e'
	WHERE color LIKE 'e';
```
Java
```java
	Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
	ArrayList<Bin> bins = new ArrayList<Bin>() {{
		    add(new Bin("ending", "ends with e"));
	}};
	Statement stmt = new Statement();
	stmt.setNamespace("test");
	stmt.setSetName("selector");
	Map<String, Long> counts = queryEngine.update(stmt, bins, qual1);
	//System.out.println(counts);
	Assert.assertEquals((Long)40L, (Long)counts.get("read"));
	Assert.assertEquals((Long)40L, (Long)counts.get("write"));
```
## Delete example
The Delete example that uses a filter, similar to this SQL:
```sql
	DELETE FROM test.people WHERE last_name='last-name-1'
```
Java
```java

	Qualifier qual1 = new Qualifier("last_name", Qualifier.FilterOperation.EQ, Value.get("last-name-1"));
	Statement stmt = new Statement();
	stmt.setNamespace("test");
	stmt.setSetName("selector");
	Map<String, Long> counts = queryEngine.delete(stmt, qual1);
```


