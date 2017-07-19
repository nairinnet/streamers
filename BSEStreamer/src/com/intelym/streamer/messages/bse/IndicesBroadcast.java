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
 * @author Hari Nair
 */
public class IndicesBroadcast extends BroadcastMessages{
    
    
    public IndicesBroadcast(){
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
        skipBytes(in, 4);
        short tradingSession = readShort(in);
        Date aDate = new Date();
        iData.timeInMillis = aDate.getTime();
        //String currentDate = sF.format(new Date());
        //iData.timeStamp = currentDate + " " + hour + ":" + minute+ ":" + second;
        //iData.timeStamp = currentDate + " | " + format(hour) + ":" + format(minute);
        iData.tradingSession = tradingSession;
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
        iData.highPrice = readInt(in);
        iData.lowPrice = readInt(in);
        iData.openPrice = readInt(in);
        iData.closePrice = readInt(in);
        iData.lastTradedPrice = readInt(in);
        iData.scripId = toChars(in, 7);
        skipBytes(in, 9);
        return iData;
    }
}
