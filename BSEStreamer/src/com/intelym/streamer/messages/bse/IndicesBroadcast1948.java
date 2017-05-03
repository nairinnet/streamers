/*
 * 1948 and 1947 same message structure
 * 
 */
package com.intelym.streamer.messages.bse;

import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.data.IData;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 *
 * @author Venkatesh
 */
public class IndicesBroadcast1948 extends BroadcastMessages {

    public IndicesBroadcast1948() {
        super();
    }

    @Override
    public boolean isLevel2Available() {
        return true;
    }

    @Override
    public IData processLevel1Messages(BSEInputStream in) throws Exception {
        IData iData = new IData();
        skipBytes(in, 2);
        iData.noOfRecords = readShort(in);
        short tradingSession = readShort(in);
        
        short hour = readShort(in);
        short minute = readShort(in);
        short second = readShort(in);
        
        skipBytes(in, 3); //filler char(3)
        
        String currentDate = sF.format(new Date());
        //iData.timeStamp = currentDate + " " + hour + ":" + minute+ ":" + second;
        iData.timeStamp = currentDate + " | " + format(hour) + ":" + format(minute);
        iData.tradingSession = tradingSession;
       
        return iData;
    }

    private String format(int i) {
        if (i < 10) {
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
        iData.lastTradedPrice = readInt(in); //Indexvalue
        iData.scripId = toChars(in, 7); //IndexId
        skipBytes(in, 1); //filler char
        return iData;
    }
}
