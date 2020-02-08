package com.ltv.aerospike.management.run;

import java.util.HashMap;

import org.apache.log4j.Level;

import com.ltv.aerospike.client.AerospikeClient;
import com.ltv.aerospike.management.form.LoginForm;
import com.ltv.core.common.util.ConfigUtils;

public class Aerospike {
    public static HashMap<String,AerospikeClient> aeConnector = new HashMap();
    public static ConfigUtils config;

    public static void main(String args[]) {
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.toLevel("INFO"));
        config = new ConfigUtils("etc");
        LoginForm.getInstance();
    }
}
