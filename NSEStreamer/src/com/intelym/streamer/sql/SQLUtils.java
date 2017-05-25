package com.intelym.streamer.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;


import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;
import com.intelym.streamer.common.Constants;
import com.intelym.streamer.common.IndicesInfo;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * SQL Utility class to load the scrip list into memory
 * @author Hari Nair
 */

public final class SQLUtils {
    private final RiwaLogger mLog = LoggerFactory.getLogger(SQLUtils.class);
    private StreamerConfiguration engineConfiguration = null;
    private String sDbUrl, sDbUser, sDbPwd, sDbDriver;
    private ConnectionPool streamerConnectionPool;
    private SimpleDateFormat sFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");
    
    
    public SQLUtils() throws Exception{
        engineConfiguration = StreamerConfiguration.getInstance();
        
        String tmp = tmp = engineConfiguration.getString(Constants.STREAMER_DB_DRIVER);
        if(tmp == null || tmp.length() == 0){
            throw new Exception("Driver not found");
        }
        sDbDriver = tmp;
        tmp = engineConfiguration.getString(Constants.STREAMER_DB_URL);
        if(tmp == null || tmp.length() == 0){
            throw new Exception("Db URL not found");
        }
        sDbUrl = tmp;
        tmp = engineConfiguration.getString(Constants.STREAMER_DB_UID);
        if(tmp == null || tmp.length() == 0){
            throw new Exception("DB User ID not found");
        }
        sDbUser = tmp;
        tmp = engineConfiguration.getString(Constants.STREAMER_DB_PWD);
        sDbPwd = tmp;
        //streamerConnectionPool = ConnectionPool.newInstance(3, 1, sDbUrl, sDbUser, sDbPwd, sDbDriver);
    }
    
    public HashMap<Integer, DerivativesScrip>  getObjectList1(HashMap<String, IndicesInfo> indexMap) {
        HashMap<Integer, DerivativesScrip> scripMap = new HashMap<>();
        String csvFile = "src/conf/contract.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try{
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                  // use comma as separator
                  String[] cm_fo_mapping = line.split(cvsSplitBy);
                  
                  String ebaStockcode = cm_fo_mapping[0];
                  String exchangeCode = cm_fo_mapping[1];
                  int cmToken;
                  String tmp = cm_fo_mapping[2].trim();
                  boolean isIndex = false;
                  //mLog.info("Mapping [ebaStockcode= " + cm_fo_mapping[1] + " , exchangeCode= " + cm_fo_mapping[2] + "cm_fo_Code= " + cm_fo_mapping[3].trim() + "]");
                  try{
                            cmToken = Integer.parseInt(tmp);
                    }catch(NumberFormatException e){
                        isIndex = true;
                    }
                  
                int focmToken = Integer.parseInt(cm_fo_mapping[3].trim());
                DerivativesScrip dScrip = new DerivativesScrip();
                dScrip.ebaStockCode = ebaStockcode;
                dScrip.exchangeCode = exchangeCode;
                dScrip.cmToken = tmp;
                dScrip.focmToken = focmToken;
                dScrip.isIndex = isIndex;
                scripMap.put(focmToken, dScrip);

              }  
        } catch (FileNotFoundException e) {
            //
        } catch (IOException e) {
            //
        }

            return scripMap;
    	
    }

    
    public HashMap<Integer, DerivativesScrip>  getObjectList(HashMap<String, IndicesInfo> indexMap) {
        HashMap<Integer, DerivativesScrip> scripMap = new HashMap<>();
        Connection connection = null;
        try{
            connection = streamerConnectionPool.borrowConnection();
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            if(connection != null){

                String strSQL = "select * from riwa_cm_fo_mapping";
                pstmt = connection.prepareStatement(strSQL);
                rs = pstmt.executeQuery();    			
                if(rs != null) {
                    while(rs.next()){ 	
                        String ebaStockcode = rs.getString(1);
                        String exchangeCode = rs.getString(2);
                        int cmToken;
                        String tmp = rs.getString(3);
                        boolean isIndex = false;
                        boolean isFuture = false;
                        try{
                            cmToken = Integer.parseInt(tmp);
                        }catch(NumberFormatException e){
                            isIndex = true;
                        }
                        int focmToken = rs.getInt(4);
                        String instrumentType = rs.getString(5);
                        if (instrumentType != null){
                            if (instrumentType.toUpperCase().startsWith("FUT")){
                                isFuture = true;
                            }
                        }
                        mLog.info("focmToken : " + focmToken);
                        DerivativesScrip dScrip = new DerivativesScrip();
                        dScrip.ebaStockCode = ebaStockcode;
                        dScrip.exchangeCode = exchangeCode;
                        dScrip.cmToken = tmp;
                        dScrip.focmToken = focmToken;
                        dScrip.isIndex = isIndex;
                        dScrip.isFuture = isFuture;
                        scripMap.put(focmToken, dScrip);
                    }
                   
                    rs.close();
                    
                }
                
                pstmt.close();
                
            }
            streamerConnectionPool.surrenderConnection(connection);

            return scripMap;
    		
    	}catch(InterruptedException | SQLException e){    		
            mLog.error("Error while retriving derivative scrip mapping results :: " + e.getMessage());
            validateAndCloseConection(connection,streamerConnectionPool);
            return null;
    	}
        
    }
    
    private void validateAndCloseConection(Connection connection ,ConnectionPool poolType)  {
    	PreparedStatement stmt = null;
        ResultSet rs =null;
         try {
            if (null != connection && connection.isValid(0)) {
                stmt = connection.prepareStatement("SELECT 1 FROM Dual");
                boolean activeConnection=false;
                rs = stmt.executeQuery();
                
                while(rs.next()) {
                	activeConnection=true;                	
             	
                }
                
                if(activeConnection){
                	stmt.close();
                	rs.close();
                	//If both succeed surrender the connection
                	poolType.surrenderConnection(connection);
                             	
                }else{
                	poolType.forcefullyCloseConnection(connection);
                	
                }
            }
        } catch(SQLException sql){
        	
        	poolType.forcefullyCloseConnection(connection);
        }
    }
}
