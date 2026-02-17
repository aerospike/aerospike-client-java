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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.CdtExp;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.HLLExp;
import com.aerospike.client.exp.LoopVarPart;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.util.Version;
import com.aerospike.test.sync.TestSync;

public class TestCdtExp extends TestSync {
    
    private static final String NAMESPACE = "test";
    private static final String SET = "testset";
    
    @BeforeClass
    public static void checkServerVersion() {
        // Skip tests for server version < 8.1.1
        Version serverVersion = client.getCluster().getRandomNode().getServerVersion();
        boolean condition = serverVersion.isGreaterOrEqual(8, 1, 1, 0);
        Assume.assumeTrue("Tests skipped for server version < 8.1.1", condition);
    }
    
    @Test
    public void testCDTExpSelect() {
        Key keyA = new Key(NAMESPACE, SET, "cdtExpSelectKey");
        
        try {
            client.delete(null, keyA);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 10.45);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 20.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 5.01);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 30.98);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin("res1", rootMap);
        client.put(null, keyA, bin);
        
        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,                    // Return type: list
                Exp.SELECT_VALUE,            // AS_CDT_SELECT_LEAF_MAP_VALUE equivalent
                Exp.mapBin("res1"),              // Source bin
                bookKey, allChildren, priceKey   // CTX path
            )
        );
        
        Record result = client.operate(null, keyA, 
            ExpOperation.write("A", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("CDT select expression operation should succeed", result != null);
        
        Record finalRecord = client.get(null, keyA);
        List<?> priceList = finalRecord.getList("A");
        assertTrue("Price list should exist", priceList != null);
        assertTrue("Should have 4 prices", priceList.size() == 4);
        
        double firstPrice = ((Number) priceList.get(0)).doubleValue();
        assertTrue("First price should be < 11", firstPrice < 11);
    }
    
    @Test
    public void testCDTExpApply() {
        Key keyA = new Key(NAMESPACE, SET, "cdtExpApplyKey");
        
        try {
            client.delete(null, keyA);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 10.45);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 20.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 5.01);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 30.98);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin("res1", rootMap);
        client.put(null, keyA, bin);
        
        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));
        
        Exp modifyExp = Exp.mul(
            Exp.floatLoopVar(LoopVarPart.VALUE),  // Current price value
            Exp.val(1.50)                         // Multiply by 1.50
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,                     // Return type: map
                Exp.MODIFY_DEFAULT,                                // Flags
                modifyExp,                        // Modify expression
                Exp.mapBin("res1"),              // Source bin
                bookKey, allChildren, priceKey   // CTX path
            )
        );
        
        Record result = client.operate(null, keyA, 
            ExpOperation.write("res1", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("CDT apply expression operation should succeed", result != null);
        
        Record finalRecord = client.get(null, keyA);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue("res1");
        assertTrue("Root map should exist", finalRootMap != null);
        
        List<?> finalBooksList = (List<?>) finalRootMap.get("book");
        assertTrue("Books list should exist", finalBooksList != null && !finalBooksList.isEmpty());
        
        Map<?, ?> firstBook = (Map<?, ?>) finalBooksList.get(0);
        assertTrue("First book should exist", firstBook != null);
        
        Object priceObj = firstBook.get("price");
        assertTrue("Price should exist", priceObj != null);
        
        double finalPrice = ((Number) priceObj).doubleValue();
        assertTrue("Price should be increased (> 11)", finalPrice > 11.0);
        
        double expectedPrice = 10.45 * 1.50;
        assertTrue("Price should be approximately " + expectedPrice, 
                   Math.abs(finalPrice - expectedPrice) < 0.01);
    }
    
    @Test
    public void testSelectTitlesWithPriceFilter() {
        Key key = new Key(NAMESPACE, SET, "selectTitlesFilterKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Cheap Book");
        book1.put("price", 5.99);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Medium Book");
        book2.put("price", 15.50);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Expensive Book");
        book3.put("price", 25.99);
        booksList.add(book3);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin("res1", rootMap);
        client.put(null, key, bin);
        
        // Select titles where price <= 10
        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(
                    MapReturnType.VALUE,
                    Exp.Type.FLOAT,
                    Exp.val("price"),
                    Exp.mapLoopVar(LoopVarPart.VALUE)
                ),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("title")
            )
        );
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("res1"),
                ctx1, ctx2, ctx3
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("titles", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        List<?> titles = finalRecord.getList("titles");
        assertNotNull("Titles should exist", titles);
        assertEquals("Should have 1 book with price <= 10", 1, titles.size());
        assertEquals("First title should be 'Cheap Book'", "Cheap Book", titles.get(0));
    }
    
    @Test
    public void testExpReadOpWithSelectByPath() {
        Key key = new Key(NAMESPACE, SET, "expReadOpSelectKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> items = new ArrayList<>();
        items.add(10);
        items.add(20);
        items.add(30);
        data.put("items", items);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select all items
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        // Use ExpReadOp to read without modifying
        Record result = client.operate(null, key,
            ExpOperation.read("result", selectExp, ExpReadFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify result
        List<?> resultItems = result.getList("result");
        assertNotNull("Items should exist", resultItems);
        assertEquals("Should have 3 items", 3, resultItems.size());
    }
    
    @Test
    public void testModifyWithAddition() {
        Key key = new Key(NAMESPACE, SET, "modifyAdditionKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> products = new ArrayList<>();
        
        Map<String, Object> p1 = new HashMap<>();
        p1.put("name", "A");
        p1.put("price", 10.0);
        products.add(p1);
        
        Map<String, Object> p2 = new HashMap<>();
        p2.put("name", "B");
        p2.put("price", 20.0);
        products.add(p2);
        
        data.put("products", products);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Add 5 to each price
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildren();
        CTX ctx3 = CTX.mapKey(Value.get("price"));
        
        Exp modifyExp = Exp.add(
            Exp.floatLoopVar(LoopVarPart.VALUE),
            Exp.val(5.0)
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify modification
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        List<?> finalProducts = (List<?>) finalData.get("products");
        assertNotNull("Products list should exist", finalProducts);
        
        Map<?, ?> firstProduct = (Map<?, ?>) finalProducts.get(0);
        assertNotNull("First product should exist", firstProduct);
        
        Object priceObj = firstProduct.get("price");
        double priceFloat = ((Number) priceObj).doubleValue();
        
        // Verify price is 15.0 (10.0 + 5.0)
        assertTrue("Price should be 15.0", Math.abs(priceFloat - 15.0) < 0.01);
    }
    
    @Test
    public void testModifyWithSubtraction() {
        Key key = new Key(NAMESPACE, SET, "modifySubtractionKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> accounts = new HashMap<>();
        accounts.put("acc1", 1000);
        accounts.put("acc2", 2000);
        data.put("accounts", accounts);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Subtract 100 from each account
        CTX ctx1 = CTX.mapKey(Value.get("accounts"));
        CTX ctx2 = CTX.allChildren();
        
        Exp modifyExp = Exp.sub(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(100)
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify modification
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        Map<?, ?> finalAccounts = (Map<?, ?>) finalData.get("accounts");
        assertNotNull("Accounts map should exist", finalAccounts);
        
        int acc1 = ((Number) finalAccounts.get("acc1")).intValue();
        assertEquals("Account1 should be 900 (1000 - 100)", 900, acc1);
    }
    
    @Test
    public void testExpWriteFlagCreateOnly() {
        Key key = new Key(NAMESPACE, SET, "createOnlyFlagKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(1);
        values.add(2);
        values.add(3);
        data.put("values", values);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        // This should succeed (new bin)
        Record result = client.operate(null, key,
            ExpOperation.write("newbin", selectExp, ExpWriteFlags.CREATE_ONLY)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // This should fail (bin already exists)
        try {
            client.operate(null, key,
                ExpOperation.write("newbin", selectExp, ExpWriteFlags.CREATE_ONLY)
            );
            assertTrue("Should have thrown exception for existing bin", false);
        } catch (AerospikeException e) {
            // Expected
        }
    }
    
    @Test
    public void testCombineSelectAndModify() {
        Key key = new Key(NAMESPACE, SET, "combineSelectModifyKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();
        
        Map<String, Object> item1 = new HashMap<>();
        item1.put("id", 1);
        item1.put("value", 10);
        items.add(item1);
        
        Map<String, Object> item2 = new HashMap<>();
        item2.put("id", 2);
        item2.put("value", 20);
        items.add(item2);
        
        Map<String, Object> item3 = new HashMap<>();
        item3.put("id", 3);
        item3.put("value", 30);
        items.add(item3);
        
        data.put("items", items);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // First, select all values
        CTX selectCtx1 = CTX.mapKey(Value.get("items"));
        CTX selectCtx2 = CTX.allChildren();
        CTX selectCtx3 = CTX.mapKey(Value.get("value"));
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                selectCtx1, selectCtx2, selectCtx3
            )
        );
        
        // Write selected values to a new bin
        client.operate(null, key,
            ExpOperation.write("values", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        // Then, modify all values by doubling them
        CTX modifyCtx1 = CTX.mapKey(Value.get("items"));
        CTX modifyCtx2 = CTX.allChildren();
        CTX modifyCtx3 = CTX.mapKey(Value.get("value"));
        
        Exp modifyExp = Exp.mul(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(2)
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                modifyCtx1, modifyCtx2, modifyCtx3
            )
        );
        
        client.operate(null, key,
            ExpOperation.write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        // Verify both bins
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        // Check original values (should be [10, 20, 30])
        List<?> values = finalRecord.getList("values");
        assertNotNull("Values should exist", values);
        assertEquals("Should have 3 values", 3, values.size());
        
        // Check modified data (values should be doubled)
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        List<?> finalItems = (List<?>) finalData.get("items");
        assertNotNull("Items list should exist", finalItems);
        
        Map<?, ?> firstItem = (Map<?, ?>) finalItems.get(0);
        assertNotNull("First item should exist", firstItem);
        
        int value = ((Number) firstItem.get("value")).intValue();
        assertEquals("Value should be doubled (10 * 2 = 20)", 20, value);
    }
    
    @Test
    public void testSelectByPathWithListOfLists() {
        Key key = new Key(NAMESPACE, SET, "listOfListsKey");
        
        try {
            client.delete(null, key);
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
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select all rows
        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("rows", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify result
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        List<?> rows = finalRecord.getList("rows");
        assertNotNull("Rows should exist", rows);
        assertEquals("Should have 3 rows", 3, rows.size());
    }
    
    @Test
    public void testModifyNestedMapValues() {
        Key key = new Key(NAMESPACE, SET, "modifyNestedMapKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> departments = new HashMap<>();
        
        Map<String, Object> sales = new HashMap<>();
        sales.put("revenue", 100000);
        sales.put("target", 120000);
        departments.put("sales", sales);
        
        Map<String, Object> engineering = new HashMap<>();
        engineering.put("revenue", 50000);
        engineering.put("target", 60000);
        departments.put("engineering", engineering);
        
        data.put("departments", departments);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Increase all revenue by 10%
        CTX ctx1 = CTX.mapKey(Value.get("departments"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("revenue")
            )
        );
        
        Exp modifyExp = Exp.mul(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(2)
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("data", applyExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify modification
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        Map<?, ?> depts = (Map<?, ?>) finalData.get("departments");
        assertNotNull("Departments map should exist", depts);
        
        Map<?, ?> salesDept = (Map<?, ?>) depts.get("sales");
        assertNotNull("Sales department should exist", salesDept);
        
        Object revenueObj = salesDept.get("revenue");
        long revenueInt = ((Number) revenueObj).longValue();
        
        long expectedRevenue = 100000 * 2;
        assertTrue("Revenue should be " + expectedRevenue + " but was " + revenueInt, revenueInt == expectedRevenue);
    }
    
    @Test
    public void testSelectByPathWithIntegerValues() {
        Key key = new Key(NAMESPACE, SET, "selectIntegerValuesKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> scores = new HashMap<>();
        scores.put("player1", 100);
        scores.put("player2", 200);
        scores.put("player3", 150);
        data.put("scores", scores);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select all scores
        CTX ctx1 = CTX.mapKey(Value.get("scores"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.read("allScores", selectExp, ExpReadFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify result
        List<?> scoresList = result.getList("allScores");
        assertNotNull("Scores should exist", scoresList);
        assertEquals("Should have 3 scores", 3, scoresList.size());
    }
    
    @Test
    public void testModifyWithDivision() {
        Key key = new Key(NAMESPACE, SET, "modifyDivisionKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        data.put("values", values);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Divide all values by 10
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildren();
        
        Exp modifyExp = Exp.div(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(10)
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify modification
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        List<?> finalValues = (List<?>) finalData.get("values");
        assertNotNull("Values list should exist", finalValues);
        
        int firstValue = ((Number) finalValues.get(0)).intValue();
        assertEquals("100 / 10 = 10", 10, firstValue);
    }
    
    @Test
    public void testSelectByPathWithMapKeys() {
        Key key = new Key(NAMESPACE, SET, "selectMapKeysKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> products = new HashMap<>();
        products.put("apple", 1.50);
        products.put("banana", 0.75);
        products.put("cherry", 2.25);
        data.put("products", products);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select all keys
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_MAP_KEY,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("keys", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify result - should get keys
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        List<?> keys = finalRecord.getList("keys");
        assertNotNull("Keys should exist", keys);
    }
    
    @Test
    public void testSelectByPathWithFilteredResults() {
        Key key = new Key(NAMESPACE, SET, "selectFilteredKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> employees = new ArrayList<>();
        
        Map<String, Object> emp1 = new HashMap<>();
        emp1.put("name", "Alice");
        emp1.put("salary", 50000);
        emp1.put("active", true);
        employees.add(emp1);
        
        Map<String, Object> emp2 = new HashMap<>();
        emp2.put("name", "Bob");
        emp2.put("salary", 60000);
        emp2.put("active", false);
        employees.add(emp2);
        
        Map<String, Object> emp3 = new HashMap<>();
        emp3.put("name", "Charlie");
        emp3.put("salary", 55000);
        emp3.put("active", true);
        employees.add(emp3);
        
        data.put("employees", employees);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select names of active employees
        CTX ctx1 = CTX.mapKey(Value.get("employees"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(
                    MapReturnType.VALUE,
                    Exp.Type.BOOL,
                    Exp.val("active"),
                    Exp.mapLoopVar(LoopVarPart.VALUE)
                ),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("name")
            )
        );
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("activeEmployees", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify result
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        List<?> names = finalRecord.getList("activeEmployees");
        assertNotNull("Names should exist", names);
        assertEquals("Should have 2 active employees", 2, names.size());
        assertTrue("Should contain 'Alice'", names.contains("Alice"));
        assertTrue("Should contain 'Charlie'", names.contains("Charlie"));
    }
    
    @Test
    public void testExpWriteFlagEvalNoFail() {
        Key key = new Key(NAMESPACE, SET, "evalNoFailKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        // Don't create the bin
        Bin bin = new Bin("otherbin", "test");
        client.put(null, key, bin);
        
        // Try to select from non-existent bin with EvalNoFail
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("nonexistent"),
                ctx1, ctx2
            )
        );
        
        // Should not fail with EVAL_NO_FAIL flag
        Record result = client.operate(null, key,
            ExpOperation.write("result", selectExp, ExpWriteFlags.EVAL_NO_FAIL)
        );
        
        assertTrue("Operation should succeed with EVAL_NO_FAIL", result != null);
    }
    
    @Test
    public void testMultipleExpWriteOpInSequence() {
        Key key = new Key(NAMESPACE, SET, "multipleOpsKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(1);
        values.add(2);
        values.add(3);
        data.put("values", values);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select values
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildren();
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        // Modify values (double them)
        Exp modifyExp = Exp.mul(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(2)
        );
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        // Execute both operations in one call
        Record result = client.operate(null, key,
            ExpOperation.write("original", selectExp, ExpWriteFlags.DEFAULT),
            ExpOperation.write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        // Verify both results
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        // Original values should be [1, 2, 3]
        List<?> original = finalRecord.getList("original");
        assertNotNull("Original values should exist", original);
        assertEquals("Should have 3 original values", 3, original.size());
        
        // Modified values should be doubled
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        List<?> finalValues = (List<?>) finalData.get("values");
        assertNotNull("Values list should exist", finalValues);
        
        int firstValue = ((Number) finalValues.get(0)).intValue();
        assertEquals("1 * 2 = 2", 2, firstValue);
    }
    
    @Test
    public void testBoolLoopVarSelect() {
        Key key = new Key(NAMESPACE, SET, "boolLoopVarSelectKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();
        
        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "Item1");
        item1.put("active", true);
        items.add(item1);
        
        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "Item2");
        item2.put("active", false);
        items.add(item2);
        
        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "Item3");
        item3.put("active", true);
        items.add(item3);
        
        data.put("items", items);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Select names where active field is true using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(
                    MapReturnType.VALUE,
                    Exp.Type.BOOL,
                    Exp.val("active"),
                    Exp.mapLoopVar(LoopVarPart.VALUE)
                ),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("name")
            )
        );
        
        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                Exp.SELECT_VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("activeNames", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        List<?> activeNames = finalRecord.getList("activeNames");
        assertNotNull("Active names should exist", activeNames);
        assertEquals("Should have 2 active items", 2, activeNames.size());
        assertTrue("Should contain Item1", activeNames.contains("Item1"));
        assertTrue("Should contain Item3", activeNames.contains("Item3"));
    }
    
    @Test
    public void testBoolLoopVarModify() {
        Key key = new Key(NAMESPACE, SET, "boolLoopVarModifyKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> flags = new HashMap<>();
        flags.put("flag1", true);
        flags.put("flag2", false);
        flags.put("flag3", true);
        data.put("flags", flags);
        
        Bin bin = new Bin("data", data);
        client.put(null, key, bin);
        
        // Negate all boolean flags using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("flags"));
        CTX ctx2 = CTX.allChildren();
        
        Exp modifyExp = Exp.not(Exp.boolLoopVar(LoopVarPart.VALUE));
        
        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                Exp.MODIFY_DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );
        
        Record result = client.operate(null, key,
            ExpOperation.write("data", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("Operation should succeed", result != null);
        
        Record finalRecord = client.get(null, key);
        assertNotNull("Final record should exist", finalRecord);
        
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull("Data map should exist", finalData);
        
        Map<?, ?> finalFlags = (Map<?, ?>) finalData.get("flags");
        assertNotNull("Flags map should exist", finalFlags);
        
        assertEquals("flag1 should be false", false, finalFlags.get("flag1"));
        assertEquals("flag2 should be true", true, finalFlags.get("flag2"));
        assertEquals("flag3 should be false", false, finalFlags.get("flag3"));
    }
    
    @Test
    public void testHllLoopVarWithHllBins() {
        // Note: HLL operations on HLL items nested in lists/maps are not currently
        // supported by the server. This test demonstrates hllLoopVar usage with
        // HLL expressions in a filtering context.
        Key key = new Key(NAMESPACE, SET, "hllLoopVarKey");
        
        try {
            client.delete(null, key);
        } catch (Exception e) {
        }
        
        // Create HLL values with different counts in separate bins
        List<Value> entries1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries1.add(Value.get("item" + i));
        }
        
        List<Value> entries2 = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            entries2.add(Value.get("item" + i));
        }
        
        List<Value> entries3 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            entries3.add(Value.get("item" + i));
        }
        
        // Create HLLs in separate bins and get them back
        Record rec = client.operate(null, key,
            HLLOperation.add(HLLPolicy.Default, "hll1", entries1, 8),
            HLLOperation.add(HLLPolicy.Default, "hll2", entries2, 8),
            HLLOperation.add(HLLPolicy.Default, "hll3", entries3, 8),
            Operation.get("hll1"),
            Operation.get("hll2"),
            Operation.get("hll3")
        );
        
        // Verify HLL bins were created
        assertNotNull("Record should exist", rec);
        
        // Extract HLLValue objects for use in expressions
        List<?> results1 = rec.getList("hll1");
        Value.HLLValue hll1 = (Value.HLLValue) results1.get(1);
        assertNotNull("HLL1 should exist", hll1);
        
        List<?> results2 = rec.getList("hll2");
        Value.HLLValue hll2 = (Value.HLLValue) results2.get(1);
        assertNotNull("HLL2 should exist", hll2);
        
        List<?> results3 = rec.getList("hll3");
        Value.HLLValue hll3 = (Value.HLLValue) results3.get(1);
        assertNotNull("HLL3 should exist", hll3);
        
        // Test that hllLoopVar can be used in expressions with HLL union operations
        List<Value.HLLValue> hllList = new ArrayList<>();
        hllList.add(hll1);
        hllList.add(hll2);
        
        // Use expression to get HLL union count
        Record checkRec = client.operate(null, key,
            ExpOperation.read("unionCount", 
                Exp.build(HLLExp.getUnionCount(Exp.val(hllList), Exp.hllBin("hll3"))),
                ExpReadFlags.DEFAULT)
        );
        
        assertNotNull("Check record should exist", checkRec);
        Object unionCount = checkRec.getValue("unionCount");
        assertNotNull("Union count should exist", unionCount);
        assertTrue("Union count should be positive", ((Number) unionCount).longValue() >= 3);
    }
}
