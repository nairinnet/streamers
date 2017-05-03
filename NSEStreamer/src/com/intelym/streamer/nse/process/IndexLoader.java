/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.nse.process;

import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;
import com.intelym.streamer.common.IndicesInfo;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 *
 * @author Hari Nair
 */
public class IndexLoader {
 
    private final String INDEX_FILE = "src/conf/index.properties";
    private static final RiwaLogger mLog = LoggerFactory.getLogger(IndexLoader.class);
    public IndexLoader(){
        
    }
    
    public HashMap<String, IndicesInfo> getIndexMap(){
        try{
            HashMap<String, IndicesInfo> indexMap = new HashMap<>();
            RandomAccessFile rFile = new RandomAccessFile(INDEX_FILE, "r");
            String tmp;
            while((tmp = rFile.readLine()) != null){
                String[] mapped = tmp.split("=");
                if(mapped.length < 2){
                    //throw new Exception("Invalid data in engine configuration information, " + tmp);
                }
                else {
                    String key = mapped[0].toUpperCase().trim();
                    int value = Integer.parseInt(mapped[1].trim());
                    IndicesInfo iInfo = new IndicesInfo(value);
                    indexMap.put(key, iInfo);
                }
            }
            return indexMap;
        }catch(IOException | NumberFormatException e){
            mLog.info("Unable to load index from index properties file " + e.getMessage());
        }
        return null;
    }
    
}
