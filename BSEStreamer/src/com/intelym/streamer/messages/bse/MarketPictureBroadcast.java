/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.bse;

import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.data.IData;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author HariNair
 */
public class MarketPictureBroadcast extends BroadcastMessages{
    
    
    public MarketPictureBroadcast(){
        super();
    }
    
    @Override
    public boolean isLevel2Available(){
        return true;
    }
    
    @Override
    public IData processLevel1Messages(BSEInputStream in) throws Exception {
        IData iData = new IData();
        skipBytes(in, 10);
        short hour = readShort(in);
        short minute = readShort(in);
        short second = readShort(in);
        skipBytes(in, 6);
        Date aDate = new Date();
        iData.timeInMillis = aDate.getTime();
        String currentDate = sF.format(aDate);
        iData.timeStamp = currentDate + " | " + format(hour) + ":" + format(minute);
     //   iData.timeStamp = currentDate + " " + hour + ":" + minute+ ":" + second;
        iData.noOfRecords = readShort(in);
        return iData;
    }
     private String format(int i){
        if(i < 10){
            return "0" + i;
        }
        return i + "";
    }
    
    @Override
    public IData processLevel2Messages(BSEInputStream in) throws Exception {
        IData iData = new IData();
        iData.scripCode = readInt(in);
        
        iData.scripId = iData.scripCode + "";
        iData.openPrice = readInt(in);
        iData.prevClosePrice = readInt(in);
        iData.highPrice = readInt(in);
        iData.lowPrice = readInt(in);
        iData.noOfTrades = readInt(in);
        iData.tradedVolume = readInt(in);
        iData.tradedValue = readInt(in);
        iData.lastTradedQty = readInt(in);
        iData.lastTradedPrice = readInt(in);
        iData.closePrice = readInt(in);
        skipBytes(in, 4);
        iData.equilibriumPrice = readInt(in);
        if(iData.lastTradedPrice == 0){
            iData.lastTradedPrice = iData.equilibriumPrice;
        }
        iData.equilibriumQty = readInt(in);
        readLong(in);
        iData.totalBidQty = readInt(in);
        iData.totalSellQty = readInt(in);
        iData.tradeValueFlag = toChars(in, 1);
        iData.trend = toChars(in, 1);
        iData.sixLakhFlag = toChars(in, 1);
        skipBytes(in, 1);
        iData.lowerCircuit = readInt(in);
        iData.upperCircuit = readInt(in);
        iData.weightedAverage = readInt(in);
        iData.marketType = readShort(in);
      //  iData.sessionNumber = readShort(in);
        iData.tradingSession = readShort(in);
        if(iData.tradingSession == 0){
            if (iData.scripCode > 1000000){
                iData.tradingSession = 3;
            }
        }
        skipBytes(in, 10);
        int noOfRecords = readShort(in);
        for(int i = 0; i < noOfRecords; i++){
            iData.mDepth[i][0] = readInt(in);
            iData.mDepth[i][1] = readInt(in);
            iData.mDepth[i][2] = readInt(in);
            readInt(in);
            iData.mDepth[i][3] = readInt(in);
            iData.mDepth[i][4] = readInt(in);
            iData.mDepth[i][5] = readInt(in);
            readInt(in);
        }
        return iData;
    }
}
