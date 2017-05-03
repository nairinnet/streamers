/*
 * Configuration owner for Riwa Streamer
 */
package com.intelym.streamer.config;

import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import javolution.util.FastList;
import javolution.util.FastMap;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.QuickLogger;

/**
 * @copyright reserved to Cazaayan Software Labs Private Ltd
 * @author Hari Nair
 * @since July 2013
 * @version 1.0
 */
public class StreamerConfiguration {
    
    private final static QuickLogger mLog = LoggerFactory.getLogger(StreamerConfiguration.class);
    private FastMap<String, String> configMap; // The Map to keep the configuration information
    private String splitter = "\\,";
    private static StreamerConfiguration eConfiguration;
    
    private StreamerConfiguration(String config) throws Exception{
        configMap = new FastMap<String, String>();
        readEngineConfiguration(config);
    }
    
    /** Create a singleton class by following methods, first with config file, second without config file 
     * @param config string the path
     * @return StreamerConfiguration the static configuration instance
     * @throws java.lang.Exception
     */
    
    public static StreamerConfiguration newInstance(String config) throws Exception{
        if(eConfiguration == null){
            mLog.info("Initializing engine configuration, configuration file is " + config);
            eConfiguration = new StreamerConfiguration(config);
        }
        return eConfiguration;
    }
    
    public static StreamerConfiguration getInstance() throws Exception{ return eConfiguration; }
    
    /**
     * Reads the configuration file for the engine from the given file as input
     * @param config the file where the configs to be read
     * @throws Exception 
     */
    private void readEngineConfiguration(String config) throws Exception{
        if(config == null) {
            throw new Exception("Invalid Streamer configuration information");
        }
        try{
            RandomAccessFile rFile = new RandomAccessFile(config, "r");
            String tmp;
            while((tmp = rFile.readLine()) != null){
                String[] mapped = tmp.split("=");
                if(mapped.length < 2){
                    //throw new Exception("Invalid data in engine configuration information, " + tmp);
                }
                else {
                    String key = mapped[0].toUpperCase().trim();
                    String value = mapped[1].trim();
                    configMap.put(key, value);
                }
            }
        }catch(Exception ex){
            System.out.println(ex);
            throw ex;
        }
    }
    
    /**
     * returns the value for the given key from the configuration
     * @param key
     * @return 
     */
    public String getString(String key){
        if(key == null) {
            return null;
        }
        return configMap.get(key.toUpperCase());
    }
    
    /**
     * caller should know when to use this method, 
     * to be used to read multiple entry data
     * @param key
     * @return 
     */
    public List getList(String key){
        if(key == null) {
            return null;
        }
        String tmp = configMap.get(key.toUpperCase());
        if(tmp == null) {
            return null;
        }
        String listableTmp[];
        listableTmp = tmp.split(splitter);
        List<String> list = new FastList<String>();
        list.addAll(Arrays.asList(listableTmp));
        return list;
    }

    public String getSplitter(){
        return (getString("Splitter") == null) ? splitter : getString("Splitter");
    }
}
