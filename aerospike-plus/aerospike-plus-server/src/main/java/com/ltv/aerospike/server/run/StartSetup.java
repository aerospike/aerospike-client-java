/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.ltv.aerospike.server.run;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.codec.digest.DigestUtils;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.query.IndexType;
import com.google.gson.Gson;
import com.ltv.aerospike.server.business.SessionBusiness;
import com.ltv.aerospike.server.repository.Connector;
import com.ltv.aerospike.server.util.AppConstant;

public class StartSetup {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(StartSetup.class.getSimpleName());
    public static Connector aeConnector;

    public void onStart() {
        if(!StartApp.config.getConfig("aerospike-host").isEmpty()) {
            aeConnector = new Connector(
                    StartApp.config.getConfig("aerospike-host"),
                    Integer.parseInt(StartApp.config.getConfig("aerospike-port")),
                    StartApp.config.getConfig("aerospike-namespace"),
                    Integer.parseInt(StartApp.config.getConfig("aerospike-max-commands-in-process")),
                    Integer.parseInt(StartApp.config.getConfig("aerospike-max-commands-in-queue")));
        }
        StartApp.aeConnector = aeConnector;

        setupDatabase();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                SessionBusiness.updateDBStructure();
            }
        }, 0, 30000);
    }

    public void onClose() {
        aeConnector.aeClient.close();
        aeConnector.eventLoops.close();
    }

    public void setupDatabase() {
        if("yes".equals(StartApp.config.getConfig("reset-database"))) {
            aeConnector.aeClient.truncate(null, StartApp.config.getConfig("aerospike-namespace"),null, null);
        }

        Key key = new Key(aeConnector.AEROSPIKE_NAMESPACE, "1", "1");
        Record info  = aeConnector.aeClient.get(null, key);
        if(info != null) return;

        insertIntoUser();
        insertIntoNamespace();
        insertIntoSet();
        insertIntoSequence();
        insertIntoIndex();
    }

    public void insertIntoUser() {
        checkAndInsert(aeConnector, AppConstant.TABLE_USER_ID, AppConstant.ROOT_USER_ID.toString(),
                       new Bin(AppConstant.KEY, AppConstant.ROOT_USER_ID),
                       new Bin("name", AppConstant.ROOT_USER_NAME),
                       new Bin("password", DigestUtils.sha256Hex(StartApp.config.getConfig("secret-key") + AppConstant.ROOT_USER_NAME)),
                       new Bin("role", AppConstant.ROLE_DBA), // dba, user
                       new Bin(AppConstant.CREATE_AT, new Date().getTime()),
                       new Bin(AppConstant.UPDATE_AT, new Date().getTime()));
    }

    public void insertIntoNamespace() {
        checkAndInsert(aeConnector, AppConstant.TABLE_NAMESPACE_ID, AppConstant.AEROSPIKE_NAMESPACE_ID.toString(),
                       new Bin(AppConstant.KEY, AppConstant.AEROSPIKE_NAMESPACE_ID),
                       new Bin("name", AppConstant.AEROSPIKE_NAMESPACE_NAME),
                       new Bin("owner", "1"), // list of namespace owner
                       new Bin(AppConstant.ROLE_DQL, ""),  // select
                       new Bin(AppConstant.ROLE_DML, ""),  // insert, update, delete, commit, rollback
                       new Bin(AppConstant.ROLE_DDL, ""),  // create, alter, drop, rename, truncate
                       new Bin(AppConstant.ROLE_DCL, ""),  // grant, revoke
                       new Bin(AppConstant.CREATE_AT, new Date().getTime()),
                       new Bin(AppConstant.UPDATE_AT, new Date().getTime()));
    }

    public void insertIntoSet() {
        Map userMeta = new HashMap();
        userMeta.put(AppConstant.KEY, AppConstant.STRING);
        userMeta.put("name", AppConstant.STRING);
        userMeta.put("password", AppConstant.STRING);
        userMeta.put("role", AppConstant.STRING);
        userMeta.put(AppConstant.CREATE_AT, AppConstant.DATE);
        userMeta.put(AppConstant.UPDATE_AT, AppConstant.DATE);
        insertSet(AppConstant.TABLE_USER_ID, AppConstant.TABLE_USER, userMeta);

        Map namespaceMeta = new HashMap();
        namespaceMeta.put(AppConstant.KEY, AppConstant.STRING);
        namespaceMeta.put("name", AppConstant.STRING);
        namespaceMeta.put("owner", AppConstant.STRING);
        namespaceMeta.put(AppConstant.ROLE_DQL, AppConstant.STRING);
        namespaceMeta.put(AppConstant.ROLE_DML, AppConstant.STRING);
        namespaceMeta.put(AppConstant.ROLE_DDL, AppConstant.STRING);
        namespaceMeta.put(AppConstant.ROLE_DCL, AppConstant.STRING);
        namespaceMeta.put(AppConstant.CREATE_AT, AppConstant.DATE);
        namespaceMeta.put(AppConstant.UPDATE_AT, AppConstant.DATE);
        insertSet(AppConstant.TABLE_NAMESPACE_ID, AppConstant.TABLE_NAMESPACE, namespaceMeta);

        Map setMap = new HashMap();
        setMap.put(AppConstant.KEY, AppConstant.STRING);
        setMap.put("name", AppConstant.STRING);
        setMap.put("namespace", AppConstant.STRING);
        setMap.put("meta", AppConstant.STRING);
        setMap.put(AppConstant.PERMISSION_SELECT, AppConstant.STRING);
        setMap.put(AppConstant.PERMISSION_PUT, AppConstant.STRING);
        setMap.put(AppConstant.PERMISSION_DELETE, AppConstant.STRING);
        setMap.put(AppConstant.DEL_FLAG, AppConstant.INTEGER);
        setMap.put(AppConstant.CREATE_AT, AppConstant.DATE);
        setMap.put(AppConstant.UPDATE_AT, AppConstant.DATE);
        insertSet(AppConstant.TABLE_SET_ID, AppConstant.TABLE_SET, setMap);

        Map sequenceMap = new HashMap();
        sequenceMap.put(AppConstant.KEY, AppConstant.STRING);
        sequenceMap.put("name", AppConstant.STRING);
        sequenceMap.put("value", AppConstant.LONG);
        sequenceMap.put("namespace", AppConstant.STRING);
        insertSet(AppConstant.TABLE_SEQUENCE_ID, AppConstant.TABLE_SEQUENCE, sequenceMap);

        Map indexMap = new HashMap();
        indexMap.put(AppConstant.KEY, AppConstant.STRING);
        indexMap.put("name", AppConstant.STRING);
        indexMap.put("namespace", AppConstant.STRING);
        indexMap.put("set", AppConstant.STRING);
        indexMap.put("bin", AppConstant.STRING);
        insertSet(AppConstant.TABLE_INDEX_ID, AppConstant.TABLE_INDEX, indexMap);
        
        Map sessionMap = new HashMap();
        sessionMap.put(AppConstant.KEY, AppConstant.STRING);
        sessionMap.put("name", AppConstant.STRING);
        sessionMap.put("role", AppConstant.STRING);
        sessionMap.put("bin", AppConstant.STRING);
        insertSet(AppConstant.TABLE_SESSION_ID, AppConstant.TABLE_SESSION, sessionMap);
    }

    public void insertSet(String id, String name, Map meta) {
        checkAndInsert(aeConnector, AppConstant.TABLE_SET_ID, id.toString(),
                       new Bin(AppConstant.KEY, id),
                       new Bin("name", name),
                       new Bin("namespace", AppConstant.AEROSPIKE_NAMESPACE_ID),
                       new Bin("meta", new Gson().toJson(meta)),
                       new Bin(AppConstant.PERMISSION_SELECT, ""),
                       new Bin(AppConstant.PERMISSION_PUT, ""),
                       new Bin(AppConstant.PERMISSION_DELETE, ""),
                       new Bin(AppConstant.DEL_FLAG, 0),
                       new Bin(AppConstant.CREATE_AT, new Date().getTime()),
                       new Bin(AppConstant.UPDATE_AT, new Date().getTime()));
    }

    public void insertIntoSequence() {
        checkAndInsert(aeConnector, AppConstant.TABLE_SEQUENCE_ID, AppConstant.TABLE_USER_ID,
                       new Bin(AppConstant.KEY, AppConstant.TABLE_USER_ID),
                       new Bin("name", AppConstant.TABLE_USER_ID),
                       new Bin("value", 1),
                       new Bin("namespace", AppConstant.AEROSPIKE_NAMESPACE_NAME));
        checkAndInsert(aeConnector, AppConstant.TABLE_SEQUENCE_ID, AppConstant.TABLE_NAMESPACE_ID,
                       new Bin(AppConstant.KEY, AppConstant.TABLE_NAMESPACE_ID),
                       new Bin("name", AppConstant.TABLE_NAMESPACE_ID),
                       new Bin("value", 1),
                       new Bin("namespace", AppConstant.AEROSPIKE_NAMESPACE_NAME));
        checkAndInsert(aeConnector, AppConstant.TABLE_SEQUENCE_ID, AppConstant.TABLE_SET_ID,
                       new Bin(AppConstant.KEY, AppConstant.TABLE_SET_ID),
                       new Bin("name", AppConstant.TABLE_SET_ID),
                       new Bin("value", 6),
                       new Bin("namespace", AppConstant.AEROSPIKE_NAMESPACE_NAME));
    }

    public void insertIntoIndex() {
        insertIndex("u_name_idx", AppConstant.AEROSPIKE_NAMESPACE_NAME, AppConstant.TABLE_USER, AppConstant.TABLE_USER_ID, "name", IndexType.STRING);
        insertIndex("ns_name_idx", AppConstant.AEROSPIKE_NAMESPACE_NAME, AppConstant.TABLE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID, "name", IndexType.STRING);
    }

    public void insertIndex(String indexName, String namespaceName, String setName, String setId, String binName, IndexType indexType) {
        checkAndInsert(aeConnector, AppConstant.TABLE_INDEX_ID, indexName,
                       new Bin(AppConstant.KEY, indexName),
                       new Bin("name", indexName),
                       new Bin("namespace", namespaceName),
                       new Bin("set", setName),
                       new Bin("bin", binName)
                       );

        try {
            aeConnector.aeClient.createIndex(null, aeConnector.AEROSPIKE_NAMESPACE, setId, indexName, binName, indexType);
        } catch (Exception ex) {
            log.info("index " + indexName + " is already exists");
        }
    }

    private void checkAndInsert(Connector ae, String setName, String keyName, Bin... bins) {
        Key key = new Key(ae.AEROSPIKE_NAMESPACE, setName, keyName);
        Record info  = ae.aeClient.get(null, key);
        if(info == null) {
            MapPolicy mPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            ae.aeClient.put(null, key, bins);
        }
    }
}
