package com.intelym.streamer.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;


import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.QuickLogger;
import com.intelym.streamer.common.Constants;
import com.intelym.streamer.common.IndicesInfo;
import java.util.HashMap;

/**
 * SQL Utility class to load the scrip list into memory
 * @author Hari Nair
 */

public final class SQLUtils {
    private final QuickLogger mLog = LoggerFactory.getLogger(SQLUtils.class);
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
        streamerConnectionPool = ConnectionPool.newInstance(3, 1, sDbUrl, sDbUser, sDbPwd, sDbDriver);
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
                        try{
                            cmToken = Integer.parseInt(tmp);
                        }catch(NumberFormatException e){
                            if(indexMap.containsKey(tmp)){
                                IndicesInfo iInfo = indexMap.get(tmp.toUpperCase().trim());
                                cmToken = iInfo.indexId;
                                isIndex = true;
                            }
                            else {
                                continue;
                            }
                        }
                        int focmToken = rs.getInt(4);
                        DerivativesScrip dScrip = new DerivativesScrip();
                        dScrip.ebaStockCode = ebaStockcode;
                        dScrip.exchangeCode = exchangeCode;
                        dScrip.cmToken = cmToken;
                        dScrip.focmToken = focmToken;
                        dScrip.isIndex = isIndex;
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
