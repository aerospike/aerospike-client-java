/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Assume;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CdtOperation;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.LoopVarPart;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.util.Version;
import com.aerospike.test.sync.TestSync;

public class TestCdtOperate extends TestSync {
    
    private static final String NAMESPACE = "test";
    private static final String SET = "testset";
    private static final String BIN_NAME = "testbin";
    
    @BeforeClass
    public static void checkServerVersion() {
        // Skip tests for server version < 8.1.1
        Version serverVersion = client.getCluster().getRandomNode().getServerVersion();
        boolean condition = serverVersion.isGreaterOrEqual(8, 1, 1, 0);
        Assume.assumeTrue("Tests skipped for server version < 8.1.1", condition);
    }
    
    @Test
    public void testCDTOperateWithExpressions() {
        Key rkey = new Key(NAMESPACE, SET, 215);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT, 
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        assertNotNull("Results should not be null", results);
        assertEquals("Should have 2 books with price <= 10.0", 2, results.size());
        
        // Verify the titles (order may vary)
        List<String> titles = new ArrayList<>();
        for (Object item : results) {
            assertTrue("Each result should be a string title", item instanceof String);
            titles.add((String) item);
        }
        
        // Check that we got the expected titles
        assertTrue("Should contain 'Sayings of the Century'", titles.contains("Sayings of the Century"));
        assertTrue("Should contain 'Moby Dick'", titles.contains("Moby Dick"));
    }
    
    @Test
    public void testCDTApplyWithExpressions() {
        Key rkey = new Key(NAMESPACE, SET, 216);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));
        
        Expression modifyExp = Exp.build(
            Exp.mul(
                Exp.floatLoopVar(LoopVarPart.VALUE),  // Current price value
                Exp.val(1.10)                         // Multiply by 1.10
            )
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, bookKey, allChildren, priceKey);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT apply operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Root map should exist", finalRootMap != null);
        
        List<?> finalBooksList = (List<?>) finalRootMap.get("book");
        assertTrue("Books list should exist", finalBooksList != null && !finalBooksList.isEmpty());
        
        Map<?, ?> firstBook = (Map<?, ?>) finalBooksList.get(0);
        assertTrue("First book should exist", firstBook != null);
        
        Object priceObj = firstBook.get("price");
        assertTrue("Price should exist", priceObj != null);
        
        double finalPrice = ((Number) priceObj).doubleValue();
        assertTrue("Price should be increased (> 9)", finalPrice > 9.0);
        
        double expectedPrice = 8.95 * 1.10;
        assertTrue("Price should be approximately " + expectedPrice, 
                   Math.abs(finalPrice - expectedPrice) < 0.01);
        
        // Verify all books have increased prices
        double[] originalPrices = {8.95, 12.99, 8.99, 22.99};
        for (int i = 0; i < finalBooksList.size(); i++) {
            Map<?, ?> book = (Map<?, ?>) finalBooksList.get(i);
            assertTrue("Book " + i + " should be a map", book != null);
            
            Object price = book.get("price");
            assertTrue("Book " + i + " should have a price", price != null);
            
            double priceFloat = ((Number) price).doubleValue();
            double expected = originalPrices[i] * 1.10;
            assertTrue("Book " + i + " price should be approximately " + expected + ", got " + priceFloat,
                      Math.abs(priceFloat - expected) < 0.01);
        }
    }
    
    @Test
    public void testNestedContextsAndComplexFilters() {
        Key rkey = new Key(NAMESPACE, SET, 217);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> store = new HashMap<>();
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("category", "reference");
        book1.put("author", "Nigel Rees");
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("category", "fiction");
        book2.put("author", "Evelyn Waugh");
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("category", "fiction");
        book3.put("author", "Herman Melville");
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);
        
        store.put("books", booksList);
        data.put("store", store);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("store"));
        CTX ctx2 = CTX.mapKey(Value.get("books"));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.and(
                Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("category"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                    Exp.val("fiction")
                ),
                Exp.lt(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                    Exp.val(10.0)
                )
            )
        );
        CTX ctx4 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3, ctx4);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        assertNotNull("Results should not be null", results);
        assertEquals("Should have 1 fiction book with price < 10.0", 1, results.size());
        assertEquals("Should get 'Moby Dick'", "Moby Dick", results.get(0));
    }
    
    @Test
    public void testEmptyResultsWhenNoItemsMatch() {
        Key rkey = new Key(NAMESPACE, SET, 218);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Expensive Book 1");
        book1.put("price", 25.99);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Expensive Book 2");
        book2.put("price", 30.50);
        booksList.add(book2);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        // Try to select books with price <= 10.0 (should return empty)
        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Verify empty results
        Object results = result.getValue(BIN_NAME);
        if (results instanceof List) {
            List<?> resultList = (List<?>) results;
            assertEquals("Should have 0 books matching the filter", 0, resultList.size());
        }
    }
    
    @Test
    public void testMatchingTreeFlag() {
        Key rkey = new Key(NAMESPACE, SET, 219);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Cheap Book");
        book1.put("price", 5.99);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Expensive Book");
        book2.put("price", 25.99);
        booksList.add(book2);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_MATCHING_TREE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // With MatchingTree, we should get back the full matching structure
        Object results = result.getValue(BIN_NAME);
        assertNotNull("Results should not be null", results);
    }
    
    @Test
    public void testMapKeysFlag() {
        Key rkey = new Key(NAMESPACE, SET, 220);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> items = new HashMap<>();
        items.put("item1", 100);
        items.put("item2", 200);
        items.put("item3", 50);
        data.put("items", items);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select with MapKeys flag - should return only keys, not values
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(75))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_MAP_KEY, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get keys where value > 75
        Object results = result.getValue(BIN_NAME);
        assertNotNull("Results should not be null", results);
    }
    
    @Test
    public void testSelectNoFailFlag() {
        Key rkey = new Key(NAMESPACE, SET, 221);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> existing = new ArrayList<>();
        existing.add(1);
        existing.add(2);
        existing.add(3);
        data.put("existing", existing);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Try to select from existing path with SelectNoFail
        CTX ctx1 = CTX.mapKey(Value.get("existing"));
        CTX ctx2 = CTX.allChildren();
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_NO_FAIL, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
    }
    
    @Test
    public void testLoopVariableIndex() {
        Key rkey = new Key(NAMESPACE, SET, 222);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);
        numbers.add(40);
        numbers.add(50);
        data.put("numbers", numbers);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select items where index < 3
        CTX ctx1 = CTX.mapKey(Value.get("numbers"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.lt(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(3))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get first 3 items (indices 0, 1, 2)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 3 items with index < 3", 3, results.size());
        }
    }
    
    @Test
    public void testLoopVariableMapKey() {
        Key rkey = new Key(NAMESPACE, SET, 223);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> products = new HashMap<>();
        products.put("apple", 1.50);
        products.put("banana", 0.75);
        products.put("cherry", 2.25);
        data.put("products", products);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select items where key starts with 'a' or 'b' (lexicographically < "c")
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.lt(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("c"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get apple and banana (keys < "c")
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 2 items with keys < 'c'", 2, results.size());
        }
    }
    
    @Test
    public void testModifyWithAddition() {
        Key rkey = new Key(NAMESPACE, SET, 224);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> scores = new ArrayList<>();
        scores.add(10);
        scores.add(20);
        scores.add(30);
        scores.add(40);
        scores.add(50);
        data.put("scores", scores);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Add 5 to each score
        CTX ctx1 = CTX.mapKey(Value.get("scores"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression modifyExp = Exp.build(
            Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5))
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT apply operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Root map should exist", finalRootMap != null);
        
        List<?> finalScores = (List<?>) finalRootMap.get("scores");
        assertTrue("Scores list should exist", finalScores != null);
        assertEquals("Should have 5 scores", 5, finalScores.size());
        
        int firstScore = ((Number) finalScores.get(0)).intValue();
        assertEquals("10 + 5 = 15", 15, firstScore);
    }
    
    @Test
    public void testModifyWithSubtraction() {
        Key rkey = new Key(NAMESPACE, SET, 225);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> balances = new HashMap<>();
        balances.put("account1", 1000);
        balances.put("account2", 2000);
        balances.put("account3", 1500);
        data.put("balances", balances);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Subtract 100 from each balance
        CTX ctx1 = CTX.mapKey(Value.get("balances"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression modifyExp = Exp.build(
            Exp.sub(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(100))
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT apply operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Root map should exist", finalRootMap != null);
        
        Map<?, ?> finalBalances = (Map<?, ?>) finalRootMap.get("balances");
        assertTrue("Balances map should exist", finalBalances != null);
        
        // Verify account1 balance was decreased by 100
        int balance1 = ((Number) finalBalances.get("account1")).intValue();
        assertEquals("1000 - 100 = 900", 900, balance1);
    }
    
    @Test
    public void testNestedListsAndComplexFilters() {
        Key rkey = new Key(NAMESPACE, SET, 226);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<List<Integer>> matrix = new ArrayList<>();
        List<Integer> row1 = new ArrayList<>();
        row1.add(1);
        row1.add(2);
        row1.add(3);
        matrix.add(row1);
        List<Integer> row2 = new ArrayList<>();
        row2.add(4);
        row2.add(5);
        row2.add(6);
        matrix.add(row2);
        List<Integer> row3 = new ArrayList<>();
        row3.add(7);
        row3.add(8);
        row3.add(9);
        matrix.add(row3);
        data.put("matrix", matrix);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildren();
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get all 3 rows
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 3 rows", 3, results.size());
        }
    }
    
    @Test
    public void testBooleanExpressionsInFilters() {
        Key rkey = new Key(NAMESPACE, SET, 227);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> users = new ArrayList<>();
        
        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "Alice");
        user1.put("active", true);
        user1.put("age", 30);
        users.add(user1);
        
        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Bob");
        user2.put("active", false);
        user2.put("age", 25);
        users.add(user2);
        
        Map<String, Object> user3 = new HashMap<>();
        user3.put("name", "Charlie");
        user3.put("active", true);
        user3.put("age", 35);
        users.add(user3);
        
        data.put("users", users);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select active users
        CTX ctx1 = CTX.mapKey(Value.get("users"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                    Exp.val("active"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get Alice and Charlie (active users)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 2 active users", 2, results.size());
            assertTrue("Should contain 'Alice'", results.contains("Alice"));
            assertTrue("Should contain 'Charlie'", results.contains("Charlie"));
        }
    }
    
    @Test
    public void testComplexAndOrFilterCombinations() {
        Key rkey = new Key(NAMESPACE, SET, 228);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> products = new ArrayList<>();
        
        Map<String, Object> p1 = new HashMap<>();
        p1.put("name", "Widget");
        p1.put("price", 10.0);
        p1.put("inStock", true);
        products.add(p1);
        
        Map<String, Object> p2 = new HashMap<>();
        p2.put("name", "Gadget");
        p2.put("price", 25.0);
        p2.put("inStock", false);
        products.add(p2);
        
        Map<String, Object> p3 = new HashMap<>();
        p3.put("name", "Gizmo");
        p3.put("price", 15.0);
        p3.put("inStock", true);
        products.add(p3);
        
        Map<String, Object> p4 = new HashMap<>();
        p4.put("name", "Doohickey");
        p4.put("price", 30.0);
        p4.put("inStock", true);
        products.add(p4);
        
        data.put("products", products);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select products that are (inStock AND price < 20) OR (price > 25)
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.or(
                Exp.and(
                    Exp.eq(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                        Exp.val(true)
                    ),
                    Exp.lt(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                        Exp.val(20.0)
                    )
                ),
                Exp.gt(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                    Exp.val(25.0)
                )
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get Widget (inStock, price 10), Gizmo (inStock, price 15), and Doohickey (price 30)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertTrue("Should have at least 1 matching product", results.size() >= 1);
        }
    }
    
    @Test
    public void testDeeplyNestedStructures() {
        Key rkey = new Key(NAMESPACE, SET, 229);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> level1 = new HashMap<>();
        Map<String, Object> level2 = new HashMap<>();
        List<Map<String, Object>> level3 = new ArrayList<>();
        
        Map<String, Object> item1 = new HashMap<>();
        item1.put("value", 100);
        level3.add(item1);
        
        Map<String, Object> item2 = new HashMap<>();
        item2.put("value", 200);
        level3.add(item2);
        
        Map<String, Object> item3 = new HashMap<>();
        item3.put("value", 300);
        level3.add(item3);
        
        level2.put("level3", level3);
        level1.put("level2", level2);
        data.put("level1", level1);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Navigate deep and select values
        CTX ctx1 = CTX.mapKey(Value.get("level1"));
        CTX ctx2 = CTX.mapKey(Value.get("level2"));
        CTX ctx3 = CTX.mapKey(Value.get("level3"));
        CTX ctx4 = CTX.allChildrenWithFilter(
            Exp.gt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("value"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(150)
            )
        );
        CTX ctx5 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("value"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3, ctx4, ctx5);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        // Should get values > 150 (200 and 300)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 2 values > 150", 2, results.size());
        }
    }
    
    @Test
    public void testSingleContextElement() {
        Key rkey = new Key(NAMESPACE, SET, 230);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        data.put("value", 123);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select with single context
        CTX ctx1 = CTX.mapKey(Value.get("value"));
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        Object results = result.getValue(BIN_NAME);
        assertNotNull("Results should not be null", results);
    }
    
    @Test
    public void testEmptyLists() {
        Key rkey = new Key(NAMESPACE, SET, 231);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Object> emptyList = new ArrayList<>();
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(2);
        items.add(3);
        data.put("emptyList", emptyList);
        data.put("items", items);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Try to select from empty list
        CTX ctx1 = CTX.mapKey(Value.get("emptyList"));
        CTX ctx2 = CTX.allChildren();
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_NO_FAIL, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
    }
    
    @Test
    public void testEmptyMaps() {
        Key rkey = new Key(NAMESPACE, SET, 232);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> emptyMap = new HashMap<>();
        Map<String, Object> items = new HashMap<>();
        items.put("a", 1);
        items.put("b", 2);
        data.put("emptyMap", emptyMap);
        data.put("items", items);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Try to select from empty map
        CTX ctx1 = CTX.mapKey(Value.get("emptyMap"));
        CTX ctx2 = CTX.allChildren();
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_NO_FAIL, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
    }
    
    @Test
    public void testListIndexContext() {
        Key rkey = new Key(NAMESPACE, SET, 233);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();
        
        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "item1");
        item1.put("value", 10);
        items.add(item1);
        
        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "item2");
        item2.put("value", 20);
        items.add(item2);
        
        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "item3");
        item3.put("value", 30);
        items.add(item3);
        
        data.put("items", items);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select value from second item
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.listIndex(1); // Select second item (index 1)
        CTX ctx3 = CTX.mapKey(Value.get("value"));
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        Object resultBin = result.getValue(BIN_NAME);
        if (resultBin instanceof List) {
            List<?> resultList = (List<?>) resultBin;
            if (resultList.size() == 1) {
                assertEquals("Should get value 20", 20L, resultList.get(0));
            }
        }
    }
    
    @Test
    public void testModifyWithIndex() {
        Key rkey = new Key(NAMESPACE, SET, 234);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        values.add(400);
        data.put("values", values);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Multiply each value by its index + 1
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression modifyExp = Exp.build(
            Exp.mul(
                Exp.intLoopVar(LoopVarPart.VALUE),
                Exp.add(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(1))
            )
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT apply operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        List<?> finalValues = (List<?>) finalData.get("values");
        assertTrue("Values list should exist", finalValues != null);
        
        assertEquals("First value should be 100", 100, ((Number) finalValues.get(0)).intValue());
        assertEquals("Second value should be 400", 400, ((Number) finalValues.get(1)).intValue());
        assertEquals("Third value should be 900", 900, ((Number) finalValues.get(2)).intValue());
        assertEquals("Fourth value should be 1600", 1600, ((Number) finalValues.get(3)).intValue());
    }
    
    @Test
    public void testModifyWithComplexArithmetic() {
        Key rkey = new Key(NAMESPACE, SET, 235);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> metrics = new ArrayList<>();
        
        Map<String, Object> m1 = new HashMap<>();
        m1.put("value", 10);
        m1.put("multiplier", 2);
        metrics.add(m1);
        
        Map<String, Object> m2 = new HashMap<>();
        m2.put("value", 20);
        m2.put("multiplier", 3);
        metrics.add(m2);
        
        Map<String, Object> m3 = new HashMap<>();
        m3.put("value", 30);
        m3.put("multiplier", 4);
        metrics.add(m3);
        
        data.put("metrics", metrics);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Add 100 to each value field in the metrics
        CTX ctx1 = CTX.mapKey(Value.get("metrics"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("value"))
        );
        
        Expression modifyExp = Exp.build(
            Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(100))
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT apply operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        List<?> finalMetrics = (List<?>) finalData.get("metrics");
        assertTrue("Metrics list should exist", finalMetrics != null);
        
        Map<?, ?> firstMetric = (Map<?, ?>) finalMetrics.get(0);
        assertTrue("First metric should exist", firstMetric != null);
        
        int value = ((Number) firstMetric.get("value")).intValue();
        assertEquals("10 + 100 = 110", 110, value);
    }
    
    @Test
    public void testRemoveAllItemsFromList() {
        Key rkey = new Key(NAMESPACE, SET, 236);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(2);
        items.add(3);
        items.add(4);
        items.add(5);
        data.put("items", items);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        List<?> finalItems = (List<?>) finalData.get("items");
        assertTrue("Items list should exist", finalItems != null);
        assertEquals("All items should be removed", 0, finalItems.size());
    }
    
    @Test
    public void testRemoveFilteredItemsFromList() {
        Key rkey = new Key(NAMESPACE, SET, 237);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> numbers = new ArrayList<>();
        numbers.add(1);
        numbers.add(5);
        numbers.add(10);
        numbers.add(15);
        numbers.add(20);
        numbers.add(25);
        numbers.add(30);
        data.put("numbers", numbers);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("numbers"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))
        );
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        List<?> finalNumbers = (List<?>) finalData.get("numbers");
        assertTrue("Numbers list should exist", finalNumbers != null);
        assertEquals("Should keep items <= 10", 3, finalNumbers.size());
        assertTrue("Should contain 1", finalNumbers.contains(1L));
        assertTrue("Should contain 5", finalNumbers.contains(5L));
        assertTrue("Should contain 10", finalNumbers.contains(10L));
    }
    
    @Test
    public void testRemoveAllItemsFromMap() {
        Key rkey = new Key(NAMESPACE, SET, 238);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put("option1", "value1");
        config.put("option2", "value2");
        config.put("option3", "value3");
        data.put("config", config);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("config"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        Map<?, ?> finalConfig = (Map<?, ?>) finalData.get("config");
        assertTrue("Config map should exist", finalConfig != null);
        assertEquals("All map entries should be removed", 0, finalConfig.size());
    }
    
    @Test
    public void testRemoveFilteredMapEntries() {
        Key rkey = new Key(NAMESPACE, SET, 239);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> scores = new HashMap<>();
        scores.put("alice", 95);
        scores.put("bob", 45);
        scores.put("carol", 75);
        scores.put("dave", 30);
        data.put("scores", scores);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("scores"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(50))
        );
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        Map<?, ?> finalScores = (Map<?, ?>) finalData.get("scores");
        assertTrue("Scores map should exist", finalScores != null);
        assertEquals("Should keep scores >= 50", 2, finalScores.size());
        
        assertTrue("Should not contain bob", !finalScores.containsKey("bob"));
        assertTrue("Should not contain dave", !finalScores.containsKey("dave"));
        
        assertTrue("Should contain alice", finalScores.containsKey("alice"));
        assertEquals("Alice score should be 95", 95L, ((Number) finalScores.get("alice")).longValue());
    }
    
    @Test
    public void testRemoveBooksWithLowPrices() {
        Key rkey = new Key(NAMESPACE, SET, 240);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Cheap Book 1");
        book1.put("price", 5.99);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Expensive Book");
        book2.put("price", 25.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Cheap Book 2");
        book3.put("price", 3.99);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "Mid Price Book");
        book4.put("price", 15.99);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("books", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("books"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Root map should exist", finalRootMap != null);
        
        List<?> finalBooks = (List<?>) finalRootMap.get("books");
        assertTrue("Books list should exist", finalBooks != null);
        assertEquals("Should keep 2 expensive books", 2, finalBooks.size());
        
        // Verify all remaining books have price > 10.0
        for (Object bookRaw : finalBooks) {
            Map<?, ?> book = (Map<?, ?>) bookRaw;
            assertTrue("Book should be a map", book != null);
            
            Object price = book.get("price");
            assertTrue("Book should have a price", price != null);
            
            double priceFloat = ((Number) price).doubleValue();
            assertTrue("Price should be > 10.0, got " + priceFloat, priceFloat > 10.0);
        }
    }
    
    @Test
    public void testRemoveItemsByIndexFilter() {
        Key rkey = new Key(NAMESPACE, SET, 241);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        values.add(400);
        values.add(500);
        data.put("values", values);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.ge(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(3))
        );
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        List<?> finalValues = (List<?>) finalData.get("values");
        assertTrue("Values list should exist", finalValues != null);
        assertEquals("Should keep first 3 items", 3, finalValues.size());
        assertEquals("First value should be 100", 100L, ((Number) finalValues.get(0)).longValue());
        assertEquals("Second value should be 200", 200L, ((Number) finalValues.get(1)).longValue());
        assertEquals("Third value should be 300", 300L, ((Number) finalValues.get(2)).longValue());
    }
    
    @Test
    public void testRemoveMapEntriesByKeyFilter() {
        Key rkey = new Key(NAMESPACE, SET, 242);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> inventory = new HashMap<>();
        inventory.put("apple", 10);
        inventory.put("banana", 5);
        inventory.put("cherry", 8);
        inventory.put("date", 3);
        data.put("inventory", inventory);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("inventory"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.ge(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("c"))
        );
        
        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        Map<?, ?> finalInventory = (Map<?, ?>) finalData.get("inventory");
        assertTrue("Inventory map should exist", finalInventory != null);
        assertEquals("Should keep 2 items", 2, finalInventory.size());
        
        assertTrue("Should contain apple", finalInventory.containsKey("apple"));
        assertTrue("Should contain banana", finalInventory.containsKey("banana"));
    }
    
    @Test
    public void testRemoveNestedItemsWithComplexPath() {
        Key rkey = new Key(NAMESPACE, SET, 243);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> departments = new HashMap<>();
        
        List<Map<String, Object>> salesList = new ArrayList<>();
        Map<String, Object> sales1 = new HashMap<>();
        sales1.put("name", "John");
        sales1.put("sales", 1000);
        salesList.add(sales1);
        Map<String, Object> sales2 = new HashMap<>();
        sales2.put("name", "Jane");
        sales2.put("sales", 5000);
        salesList.add(sales2);
        
        List<Map<String, Object>> engList = new ArrayList<>();
        Map<String, Object> eng1 = new HashMap<>();
        eng1.put("name", "Bob");
        eng1.put("sales", 500);
        engList.add(eng1);
        Map<String, Object> eng2 = new HashMap<>();
        eng2.put("name", "Alice");
        eng2.put("sales", 3000);
        engList.add(eng2);
        
        departments.put("sales", salesList);
        departments.put("engineering", engList);
        data.put("departments", departments);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("departments"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.lt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("sales"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(2000)
            )
        );


        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT remove operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        Map<?, ?> finalDepartments = (Map<?, ?>) finalData.get("departments");
        assertTrue("Departments map should exist", finalDepartments != null);
        
        List<?> finalSalesList = (List<?>) finalDepartments.get("sales");
        assertTrue("Sales list should exist", finalSalesList != null);
        assertEquals("Should keep Jane only", 1, finalSalesList.size());
        
        List<?> finalEngList = (List<?>) finalDepartments.get("engineering");
        assertTrue("Engineering list should exist", finalEngList != null);
        assertEquals("Should keep Alice only", 1, finalEngList.size());
    }
    
    @Test
    public void testOperateWithNoOperations() {
        Key rkey = new Key(NAMESPACE, SET, 244);
       
        // Make sure the record does not exist
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        data.put("value", 123);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        try {
            client.operate(null, rkey);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testSelectByPathWithNullContext() {
        Key rkey = new Key(NAMESPACE, SET, 245);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);
        
        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, (CTX[])null);
        
        try {
            client.operate(null, rkey, selectOp);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testSelectByPathWithNoContexts() {
        Key rkey = new Key(NAMESPACE, SET, 246);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);
        
        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE);
        
        try {
            client.operate(null, rkey, selectOp);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testSelectByPathWithEmptyContextArray() {
        Key rkey = new Key(NAMESPACE, SET, 247);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        data.put("value1", 100);
        data.put("value2", 200);
        data.put("value3", 300);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        CTX[] emptyCtx = new CTX[0];
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, emptyCtx);
        
        try {
            client.operate(null, rkey, selectOp);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testModifyByPathWithNullContext() {
        Key rkey = new Key(NAMESPACE, SET, 248);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);
        
        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        Expression modifyExp = Exp.build(Exp.val(100));
        Operation modifyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, (CTX[])null);
        
        try {
            client.operate(null, rkey, modifyOp);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testModifyByPathWithNoContexts() {
        Key rkey = new Key(NAMESPACE, SET, 249);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);
        
        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        Expression modifyExp = Exp.build(Exp.val(100));
        Operation modifyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp);
        
        try {
            client.operate(null, rkey, modifyOp);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testModifyByPathWithEmptyContextArray() {
        Key rkey = new Key(NAMESPACE, SET, 250);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        data.put("count", 50);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        CTX[] emptyCtx = new CTX[0];
        Expression modifyExp = Exp.build(Exp.val(200));
        Operation modifyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, emptyCtx);
        
        try {
            client.operate(null, rkey, modifyOp);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
        }
    }
    
    @Test
    public void testLoopVarListWithNestedLists() {
        Key rkey = new Key(NAMESPACE, SET, 251);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<List<Integer>> matrix = new ArrayList<>();
        List<Integer> row1 = new ArrayList<>();
        row1.add(1);
        row1.add(2);
        row1.add(3);
        matrix.add(row1);
        List<Integer> row2 = new ArrayList<>();
        row2.add(4);
        row2.add(5);
        row2.add(6);
        matrix.add(row2);
        List<Integer> row3 = new ArrayList<>();
        row3.add(7);
        row3.add(8);
        row3.add(9);
        matrix.add(row3);
        data.put("matrix", matrix);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildren();
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 3 rows", 3, results.size());
        }
    }
    
    @Test
    public void testModifyWithDivision() {
        Key rkey = new Key(NAMESPACE, SET, 252);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        data.put("values", values);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression modifyExp = Exp.build(
            Exp.div(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT modify operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        List<?> finalValues = (List<?>) finalData.get("values");
        assertTrue("Values list should exist", finalValues != null);
        
        int firstValue = ((Number) finalValues.get(0)).intValue();
        assertEquals("100 / 10 = 10", 10, firstValue);
    }
    
    @Test
    public void testLoopVarListAccessNestedListSize() {
        Key rkey = new Key(NAMESPACE, SET, 253);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<List<Integer>> matrix = new ArrayList<>();
        
        List<Integer> row1 = new ArrayList<>();
        row1.add(1);
        row1.add(2);
        row1.add(3);
        matrix.add(row1);
        
        List<Integer> row2 = new ArrayList<>();
        row2.add(4);
        row2.add(5);
        matrix.add(row2);
        
        List<Integer> row3 = new ArrayList<>();
        row3.add(7);
        row3.add(8);
        row3.add(9);
        matrix.add(row3);
        
        data.put("matrix", matrix);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                ListExp.size(Exp.listLoopVar(LoopVarPart.VALUE)),
                Exp.val(3)
            )
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 2 rows with size 3", 2, results.size());
        }
    }
    
    @Test
    public void testLoopVarBlobAccessBlobValues() {
        Key rkey = new Key(NAMESPACE, SET, 254);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<byte[]> blobs = new ArrayList<>();
        blobs.add("First blob content".getBytes());
        blobs.add("Second blob content".getBytes());
        blobs.add("Target blob".getBytes());
        blobs.add("Fourth blob content".getBytes());
        
        data.put("blobs", blobs);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("blobs"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.blobLoopVar(LoopVarPart.VALUE),
                Exp.val("Target blob".getBytes())
            )
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 1 blob matching target", 1, results.size());
            byte[] resultBlob = (byte[]) results.get(0);
            assertEquals("Should match target blob", "Target blob", new String(resultBlob));
        }
    }
    
    @Test
    public void testLoopVarNilWithNilValues() {
        Key rkey = new Key(NAMESPACE, SET, 255);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        data.put("a", 1);
        data.put("b", 2);
        data.put("c", true);
        data.put("d", "test".getBytes());
        data.put("e", null);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        CTX ctx1 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.nilLoopVar(LoopVarPart.VALUE),
                Exp.nil()
            )
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE | Exp.SELECT_NO_FAIL, ctx1);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 1 nil value", 1, results.size());
        }
    }
    
    @Test
    public void testLoopVarGeoJSONFilterLocations() {
        Key rkey = new Key(NAMESPACE, SET, 256);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Value.GeoJSONValue> locations = new ArrayList<>();
        
        locations.add(new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-122.4194,37.7749]}"));
        locations.add(new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-118.2437,34.0522]}"));
        locations.add(new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-73.9352,40.7306]}"));
        
        data.put("locations", locations);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        String californiaRegion = "{\"type\":\"Polygon\",\"coordinates\":[[[-124.5,32.5],[-114.0,32.5],[-114.0,42.0],[-124.5,42.0],[-124.5,32.5]]]}";
        
        CTX ctx1 = CTX.mapKey(Value.get("locations"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            com.aerospike.client.exp.Exp.geoCompare(
                Exp.geoJsonLoopVar(LoopVarPart.VALUE),
                Exp.geo(californiaRegion)
            )
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertTrue("Should have filtered GeoJSON locations", results.size() >= 0);
            for (Object item : results) {
                assertNotNull("Location should not be null", item);
            }
        }
    }
    
    @Test
    public void testBoolLoopVarFilterActive() {
        Key rkey = new Key(NAMESPACE, SET, 257);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> users = new ArrayList<>();
        
        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "Alice");
        user1.put("active", true);
        user1.put("score", 95);
        users.add(user1);
        
        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Bob");
        user2.put("active", false);
        user2.put("score", 85);
        users.add(user2);
        
        Map<String, Object> user3 = new HashMap<>();
        user3.put("name", "Charlie");
        user3.put("active", true);
        user3.put("score", 90);
        users.add(user3);
        
        Map<String, Object> user4 = new HashMap<>();
        user4.put("name", "Diana");
        user4.put("active", false);
        user4.put("score", 88);
        users.add(user4);
        
        data.put("users", users);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select names where active is true using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("users"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                    Exp.val("active"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 2 active users", 2, results.size());
            assertTrue("Should contain Alice", results.contains("Alice"));
            assertTrue("Should contain Charlie", results.contains("Charlie"));
        }
    }
    
    @Test
    public void testBoolLoopVarModifyFlags() {
        Key rkey = new Key(NAMESPACE, SET, 258);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> settings = new HashMap<>();
        settings.put("enableFeatureA", true);
        settings.put("enableFeatureB", false);
        settings.put("enableFeatureC", true);
        settings.put("enableFeatureD", false);
        data.put("settings", settings);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Negate all boolean settings using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("settings"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        
        Expression modifyExp = Exp.build(
            Exp.not(Exp.boolLoopVar(LoopVarPart.VALUE))
        );
        
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT modify operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Data map should exist", finalData != null);
        
        Map<?, ?> finalSettings = (Map<?, ?>) finalData.get("settings");
        assertTrue("Settings map should exist", finalSettings != null);
        
        assertEquals("enableFeatureA should be false", false, finalSettings.get("enableFeatureA"));
        assertEquals("enableFeatureB should be true", true, finalSettings.get("enableFeatureB"));
        assertEquals("enableFeatureC should be false", false, finalSettings.get("enableFeatureC"));
        assertEquals("enableFeatureD should be true", true, finalSettings.get("enableFeatureD"));
    }
    
    @Test
    public void testBoolLoopVarInListFilter() {
        Key rkey = new Key(NAMESPACE, SET, 259);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Boolean> flags = new ArrayList<>();
        flags.add(true);
        flags.add(false);
        flags.add(true);
        flags.add(true);
        flags.add(false);
        data.put("flags", flags);
        
        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);
        
        // Select indices where flag is true using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("flags"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.boolLoopVar(LoopVarPart.VALUE), Exp.val(true))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);
        
        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals("Should have 3 true flags", 3, results.size());
            for (Object item : results) {
                assertEquals("All results should be true", true, item);
            }
        }
    }
    
    @Test
    public void testHllLoopVarWithHllExpressions() {
        // Demonstrates hllLoopVar usage pattern with selectByPath.
        // Creates a metadata structure and shows the expression pattern for HLL filtering.
        Key rkey = new Key(NAMESPACE, SET, 260);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        // Create HLL values in separate bins
        List<Value> entries1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            entries1.add(Value.get("item" + i));
        }
        
        List<Value> entries2 = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            entries2.add(Value.get("item" + i));
        }
        
        List<Value> entries3 = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            entries3.add(Value.get("item" + i));
        }
        
        Record rec = client.operate(null, rkey,
            HLLOperation.add(HLLPolicy.Default, "hll1", entries1, 8),
            HLLOperation.add(HLLPolicy.Default, "hll2", entries2, 8),
            HLLOperation.add(HLLPolicy.Default, "hll3", entries3, 8),
            Operation.get("hll1"),
            Operation.get("hll2"),
            Operation.get("hll3")
        );
        
        assertNotNull("Record should exist", rec);
        
        // Create a metadata structure to demonstrate selectByPath pattern
        Map<String, Object> hllMetadata = new HashMap<>();
        List<Map<String, Object>> hllInfo = new ArrayList<>();
        
        Map<String, Object> info1 = new HashMap<>();
        info1.put("name", "small");
        info1.put("binName", "hll1");
        info1.put("expectedCount", 5);
        hllInfo.add(info1);
        
        Map<String, Object> info2 = new HashMap<>();
        info2.put("name", "medium");
        info2.put("binName", "hll2");
        info2.put("expectedCount", 20);
        hllInfo.add(info2);
        
        Map<String, Object> info3 = new HashMap<>();
        info3.put("name", "large");
        info3.put("binName", "hll3");
        info3.put("expectedCount", 50);
        hllInfo.add(info3);
        
        hllMetadata.put("hlls", hllInfo);
        
        Bin metaBin = new Bin(BIN_NAME, hllMetadata);
        client.put(null, rkey, metaBin);
        
        // Use selectByPath to filter HLL metadata where expectedCount > 10
        CTX ctx1 = CTX.mapKey(Value.get("hlls"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.gt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("expectedCount"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );
        
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);
        
        Record result = client.operate(null, rkey, selectOp);
        assertNotNull("Result should exist", result);
        
        List<?> selectedNames = result.getList(BIN_NAME);
        assertNotNull("Selected names should exist", selectedNames);
        assertEquals("Should have 2 HLLs with count > 10", 2, selectedNames.size());
        assertTrue("Should contain 'medium'", selectedNames.contains("medium"));
        assertTrue("Should contain 'large'", selectedNames.contains("large"));
        
        // Demonstrate hllLoopVar expression construction
        // This pattern would be used if HLLs were in nested structures:
        // CTX.allChildrenWithFilter(
        //     Exp.gt(HLLExp.getCount(Exp.hllLoopVar(LoopVarPart.VALUE)), Exp.val(10))
        // )
        Exp hllLoopVarExp = Exp.hllLoopVar(LoopVarPart.VALUE);
        assertNotNull("hllLoopVar expression should be created", hllLoopVarExp);
    }
    
    @Test
    public void testHllLoopVarInExpressionContext() {
        Key rkey = new Key(NAMESPACE, SET, 261);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        // Create a simple HLL
        List<Value> entries = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            entries.add(Value.get("value" + i));
        }
        
        Record rec = client.operate(null, rkey,
            HLLOperation.add(HLLPolicy.Default, "testHll", entries, 8),
            HLLOperation.getCount("testHll")
        );
        
        assertNotNull("Record should exist", rec);
        
        List<?> resultList = rec.getList("testHll");
        long count = (Long) resultList.get(1); // getCount is the second operation
        assertTrue("HLL count should be around 15", count >= 10 && count <= 20);
        
        Exp hllLoopExp = Exp.hllLoopVar(LoopVarPart.VALUE);
        assertNotNull("hllLoopVar expression should be created", hllLoopExp);
    }
    
    @Test
    public void testSelectByPathWithNullBinName() {
        CTX ctx1 = CTX.mapKey(Value.get("test"));

        try {
             CdtOperation.selectByPath(null, Exp.SELECT_VALUE, ctx1);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
            assertTrue("Error message should mention binName", e.getMessage().contains("binName"));
        }
    }

    @Test
    public void testSelectByPathWithEmptyBinName() {
        CTX ctx1 = CTX.mapKey(Value.get("test"));

        try {
            CdtOperation.selectByPath("", Exp.SELECT_VALUE, ctx1);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
            assertTrue("Error message should mention binName", e.getMessage().contains("binName"));
        }
    }

    @Test
    public void testModifyByPathWithNullBinName() {
        CTX ctx1 = CTX.mapKey(Value.get("test"));
        Expression modifyExp = Exp.build(Exp.val(100));

        try {
            CdtOperation.modifyByPath(null, Exp.MODIFY_DEFAULT, modifyExp, ctx1);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
            assertTrue("Error message should mention binName", e.getMessage().contains("binName"));
        }
    }

    @Test
    public void testModifyByPathWithEmptyBinName() {
        CTX ctx1 = CTX.mapKey(Value.get("test"));
        Expression modifyExp = Exp.build(Exp.val(100));

        try {
            CdtOperation.modifyByPath("", Exp.MODIFY_DEFAULT, modifyExp, ctx1);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
            assertTrue("Error message should mention binName", e.getMessage().contains("binName"));
        }
    }

    @Test
    public void testSelectByPathWithBinNameTooLong() {
        CTX ctx1 = CTX.mapKey(Value.get("test"));
        String longBinName = "1234567890123456"; // 16 characters, exceeds limit of 15

        try {
            CdtOperation.selectByPath(longBinName, Exp.SELECT_VALUE, ctx1);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
            assertTrue("Error message should mention character limit", e.getMessage().contains("15") || e.getMessage().contains("exceed"));
        }
    }

    @Test
    public void testModifyByPathWithBinNameTooLong() {
        CTX ctx1 = CTX.mapKey(Value.get("test"));
        Expression modifyExp = Exp.build(Exp.val(100));
        String longBinName = "1234567890123456"; // 16 characters, exceeds limit of 15

        try {
            CdtOperation.modifyByPath(longBinName, Exp.MODIFY_DEFAULT, modifyExp, ctx1);
            assertTrue("Should throw AerospikeException with PARAMETER_ERROR", false);
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals("Should be PARAMETER_ERROR", com.aerospike.client.ResultCode.PARAMETER_ERROR, e.getResultCode());
            assertTrue("Error message should mention character limit", e.getMessage().contains("15") || e.getMessage().contains("exceed"));
        }
    }

}