/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.business;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.lua.LuaAerospikeLib.log;
import com.aerospike.client.policy.ScanPolicy;
import com.ltv.aerospike.server.run.StartApp;
import com.ltv.aerospike.server.util.AppConstant;

public class SessionBusiness {

    public static HashMap<String, HashMap> sessions = new HashMap();
    public static HashMap<String, Record> namespaces = new HashMap();
    public static HashMap<String, Record> sets = new HashMap();
    public static HashMap<String, Record> indexs = new HashMap();
    public static HashMap<String, Record> sequences = new HashMap();
    public static HashMap<String, Record> users = new HashMap();

    public static void put(String token, String userId, String userName, String role) {
        HashMap session = new HashMap();
        session.put("token", token);
        session.put("userId", userId);
        session.put("userName", userName);
        session.put("role", role);
        sessions.put(token, session);
    }

    public static String getUserName(String token) {
        HashMap session = sessions.get(token);
        return (String) session.get("userName");
    }

    public static String getRole(String token) {
        HashMap session = sessions.get(token);
        return (String) session.get("role");
    }

    public static String getUserId(String token) {
        HashMap session = sessions.get(token);
        return (String) session.get("userId");
    }

    public static HashMap get(String token) {
        return sessions.get(token);
    }

    public static void invalidate(String token) {
        sessions.remove(token);
    }

    public static boolean contain(String token) {
        return sessions.containsKey(token);
    }

    public static boolean hasRole(String token, String role) {
        return hasRole(token, role, null);
    }

    public static boolean hasRole(String token, String role, Record namespaceRecord) {
        if(AppConstant.ROLE_DBA.equals(getRole(token))) return true;

        if(namespaceRecord == null) return false;
        if(namespaceRecord.getString(AppConstant.ROLE_OWNER) != null && !namespaceRecord.getString(AppConstant.ROLE_OWNER).isEmpty()) {
            List owner = Arrays.asList(namespaceRecord.getString(AppConstant.ROLE_OWNER).split(","));
            if (owner.contains(getUserId(token)))
                return true;
        }

        if(namespaceRecord.getString(role) == null && !namespaceRecord.getString(role).isEmpty()) return false;
        List roles = Arrays.asList(namespaceRecord.getString(role).split(","));
        return roles.contains(getUserId(token));
    }

    public static boolean hasPermission(String token, Record namespaceRecord, Record setRecord, String permission) {
        if(AppConstant.PERMISSION_SELECT.equals(permission)) {
            if(hasRole(token, AppConstant.ROLE_DBA)
               || hasRole(token, AppConstant.ROLE_OWNER, namespaceRecord)
               || hasRole(token, AppConstant.ROLE_DQL, namespaceRecord))
                return true;
        } else {
            if(hasRole(token, AppConstant.ROLE_DBA)
               || hasRole(token, AppConstant.ROLE_OWNER, namespaceRecord)
               || hasRole(token, AppConstant.ROLE_DML, namespaceRecord))
                return true;
        }
        List selectUsers = Arrays.asList(setRecord.getString(permission).split(","));
        return selectUsers.contains(getUserId(token));
    }

    public static void updateDBStructure() {
        loadNamespaces();
        loadSets();
        loadIndexs();
        loadSequences();
        loadUsers();
        loadSession();
    }

    private static void loadSession() {
        StartApp.aeConnector.aeClient.scanAll(StartApp.aeConnector.eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                HashMap session = new HashMap();
                String token = record.getString("token");
                session.put("token", token);
                session.put("userId", record.getString(AppConstant.KEY));
                session.put("userName", record.getString("name"));
                session.put("role", record.getString("role"));
                sessions.put(token, session);
            }

            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(AerospikeException e) {
                log.error("Response failed: " + e);
            }
        }, new ScanPolicy(), StartApp.aeConnector.AEROSPIKE_NAMESPACE, AppConstant.TABLE_SESSION_ID.toString());
    }

    private static void loadNamespaces() {
        StartApp.aeConnector.aeClient.scanAll(StartApp.aeConnector.eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                if(1 == record.getLong(AppConstant.DEL_FLAG)) return;
                namespaces.put(record.getString("name"), record);
            }

            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(AerospikeException e) {
                log.error("Response failed: " + e);
            }
        }, new ScanPolicy(), StartApp.aeConnector.AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID);
    }

    private static void loadSets() {
        StartApp.aeConnector.aeClient.scanAll(StartApp.aeConnector.eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                if(1 == record.getLong(AppConstant.DEL_FLAG)) return;
                sets.put(record.getString("namespace") + "_" + record.getString("name"), record);
            }

            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(AerospikeException e) {
                log.error("Response failed: " + e);
            }
        }, new ScanPolicy(), StartApp.aeConnector.AEROSPIKE_NAMESPACE, AppConstant.TABLE_SET_ID);
    }

    private static void loadIndexs() {
        StartApp.aeConnector.aeClient.scanAll(StartApp.aeConnector.eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                if(1 == record.getLong(AppConstant.DEL_FLAG)) return;
                indexs.put(record.getString("name"), record);
            }

            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(AerospikeException e) {
                log.error("Response failed: " + e);
            }
        }, new ScanPolicy(), StartApp.aeConnector.AEROSPIKE_NAMESPACE, AppConstant.TABLE_INDEX_ID);
    }

    private static void loadSequences() {
        StartApp.aeConnector.aeClient.scanAll(StartApp.aeConnector.eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                if(1 == record.getLong(AppConstant.DEL_FLAG)) return;
                sequences.put(record.getString("name"), record);
            }

            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(AerospikeException e) {
                log.error("Response failed: " + e);
            }
        }, new ScanPolicy(), StartApp.aeConnector.AEROSPIKE_NAMESPACE, AppConstant.TABLE_SEQUENCE_ID);
    }

    public static void loadUsers() {
        StartApp.aeConnector.aeClient.scanAll(StartApp.aeConnector.eventLoops.next(), new RecordSequenceListener() {
            @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                if(1 == record.getLong(AppConstant.DEL_FLAG)) return;
                users.put(String.valueOf(record.getString(AppConstant.KEY)), record);
            }

            @Override
            public void onSuccess() {
            }

            @Override
            public void onFailure(AerospikeException e) {
                log.error("Response failed: " + e);
            }
        }, new ScanPolicy(), StartApp.aeConnector.AEROSPIKE_NAMESPACE, AppConstant.TABLE_USER_ID);
    }
}
