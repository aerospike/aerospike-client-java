package com.aerospike.client.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import com.aerospike.client.Log;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.aerospike.client.configuration.serializers.Configuration;
import org.yaml.snakeyaml.error.YAMLException;

public class YamlConfigProvider implements ConfigurationProvider {
    private static final String configurationPathEnv = "CONFIGURATION_PATH";
    private static final String configurationPathProp = "configuration.path";
    private static final String yamlSerializersPath = "com.aerospike.client.configuration.serializers.";
    private static String configurationPath = System.getenv().getOrDefault(configurationPathEnv, System.getProperty(configurationPathProp, System.getProperty("user.dir")));
    private Configuration configuration;
    public long lastModified;

    public YamlConfigProvider() {
        loadConfiguration();
    }

    public YamlConfigProvider(String configFilePath) {
        configurationPath = configFilePath;
        loadConfiguration();
    }

    public Configuration fetchConfiguration() {
        return configuration;
    }

    public Configuration fetchDynamicConfiguration() {
        configuration.staticConfiguration = null;
        return configuration;
    }

    public void loadConfiguration() {
        ConfigurationTypeDescription configurationTypeDescription = new ConfigurationTypeDescription();
        LoaderOptions yamlLoaderOptions = new LoaderOptions();

        Map<Class<?>, TypeDescription> typeDescriptions = configurationTypeDescription.buildTypeDescriptions(yamlSerializersPath, Configuration.class);
        Constructor typeDescriptionConstructor = new Constructor(Configuration.class, yamlLoaderOptions);
        Yaml yaml = new Yaml(typeDescriptionConstructor);

        typeDescriptions.values().forEach(typeDescriptionConstructor::addTypeDescription);
        try(FileInputStream fileInputStream = new FileInputStream(configurationPath)) {
            File file = new File(configurationPath);
            long newLastModified = file.lastModified();
            if (newLastModified > lastModified) {
                lastModified = newLastModified;
                Log.debug("YAML config file has been modified.  Loading...");
                configuration = yaml.load(fileInputStream);
            }
            else {
                Log.debug("YAML config file has NOT been modified.  NOT loading.");
            }
        } catch (FileNotFoundException e) {
            Log.error("Configuration file could not be read from" + configurationPath + ". Will fall back to default settings.");
        } catch (IOException ioException) {
            Log.error("Configuration file could not be read from" + configurationPath + ". Make sure configuration is valid YAML file. Will fall back to default settings");
        } catch (YAMLException e) {
            Log.error("Unable to parse YAML file: " + e.getMessage());
        }
    }
}
