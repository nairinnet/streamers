/*
 * Database connection manager to read the sensex scrips
 */
package com.intelym.database;

import com.intelym.streamer.nse.process.ScripInfo;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;
import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.streamer.data.ScripData;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 *
 * @author Nirmal Roychowdhury
 * @version 1.0
 *
 */
public class ConnectionManager {
    private static RiwaLogger mLog = LoggerFactory.getLogger(ConnectionManager.class);
    private StreamerConfiguration eConfiguration = null;
    private Connection connection = null;
    public ConnectionManager(StreamerConfiguration eConfig){
        eConfiguration = eConfig;
    }
    
    public void connect(){
        try{
             // API conncection will come here: 
        }catch(Exception e){
            mLog.info("ConnectionManager : connect : "+e.getLocalizedMessage());
            
        }
    }
    
    public void close(){
        try{
            if(connection != null){
                connection.close();
            }
        }catch(Exception e){
            mLog.info("ConnectionManager : close : "+e.getLocalizedMessage());
        }
    }
    private ScripInfo getScrip(String exchange, String scripID){
        return new ScripInfo(exchange, scripID);
    }
    
    public TreeMap getIndexScripList(){
        final TreeMap<String,ArrayList> scripMap = new TreeMap<String,ArrayList>();
        try{
            BufferedReader in = new BufferedReader(new FileReader("D:\\Cazaayan\\ScripDetails.txt"));
            String line;
            while((line = in.readLine())!=null){
                String [] scripDetails = line.split(",");
                String Group = scripDetails[0];
                String scripCode = scripDetails[1];
                if(scripCode != null){
                    ArrayList scripList = scripMap.get(Group);
                    if(scripList==null){
                        scripList = new ArrayList();
                    }
                    if(!scripList.contains(new Integer(scripCode)))
                        scripList.add(new Integer(scripCode));
                    scripMap.put(Group, scripList);
                }
            }
            in.close();
            mLog.info("Loaded Index scrip for Time based feeds");
            
        }catch(Exception e){ 
            mLog.error("No Index details are added");
        }
        return scripMap;
    }
 
}
