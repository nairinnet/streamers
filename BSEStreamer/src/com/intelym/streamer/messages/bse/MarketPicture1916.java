/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.bse;

import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.data.IData;
import java.util.Date;

/**
 *
 * @author Venkatesh
 */
public class MarketPicture1916 extends BroadcastMessages {

    public MarketPicture1916() {
        super();
    }

    @Override
    public boolean isLevel2Available() {
        return true;
    }

    @Override
    public IData processLevel1Messages(BSEInputStream in) throws Exception {
        IData iData = new IData();
        skipBytes(in, 4);
        iData.tradingSession = readShort(in);
        iData.noOfRecords = readShort(in);
        short hour = readShort(in);
        short minute = readShort(in);
        short second = readShort(in);
        skipBytes(in, 6);
        Date aDate = new Date();
        iData.timeInMillis = aDate.getTime();
        String currentDate = sF.format(aDate);
        iData.timeStamp = currentDate + " | " + format(hour) + ":" + format(minute);       
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
        iData.scripId = iData.scripCode + "";

        iData.noOfTrades = readInt(in);
        iData.tradedVolume = readInt(in);
        iData.tradedValue = readInt(in);
        iData.tradeValueFlag = toChars(in, 1);
        iData.trend = toChars(in, 1);
        iData.sixLakhFlag = toChars(in, 1);
        skipBytes(in, 1);
        iData.lastTradedQty = readInt(in);
        iData.lastTradedPrice = readInt(in);
        skipBytes(in, 8);
        iData.openPrice = readInt(in);
        iData.closePrice = readInt(in);
        iData.highPrice = readInt(in);
        iData.lowPrice = readInt(in);
        iData.totalBidQty = readInt(in);
        iData.totalSellQty = readInt(in);

        iData.lowerCircuit = readInt(in);
        iData.upperCircuit = readInt(in);
        iData.weightedAverage = readInt(in);
        
        skipBytes(in, 40);
        return iData;
    }
}
