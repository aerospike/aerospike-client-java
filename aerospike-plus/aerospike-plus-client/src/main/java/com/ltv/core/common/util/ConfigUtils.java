/*
 * Copyright (c) 2019 LINE Corporation. All rights reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.ltv.core.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * @since 11/03/2014
 * @author HienDM
 */

public class ConfigUtils {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(ConfigUtils.class.getSimpleName());
    private String CONFIG_PATH;
    private Properties config;
    public String configPath = "";
    
    public ConfigUtils(String path) {
        loadAllConfig(path);
    }
    
    public void loadAllConfig(String path) {
        configPath = path;
        CONFIG_PATH = configPath + "/server.conf";
        config = loadConfig();
    }
    
    public Properties loadConfig() {
        Properties prop = new Properties();
        File file = new File(CONFIG_PATH);
        if (file.exists()) {        
            try (FileInputStream inputStream = new FileInputStream(CONFIG_PATH)){
                prop.load(inputStream);
            } catch (Exception ex) {
                log.error("Error when load server.conf", ex);
            }
        }
        return prop;
    }
        
    public String getConfig(String key) {
        String value = (String) config.get(key);
        if(value == null) value = "";
        return value;
    }
}

