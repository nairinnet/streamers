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
 * @author Venkatesh
 */
public class MarketPicture1906 extends BroadcastMessages {

    public MarketPicture1906() {
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
        
      /* no time */
        
        iData.tradingSession = readShort(in);
    /*    if (iData.tradingSession == 0) {
            if (iData.scripCode > 1000000) {
                iData.tradingSession = 3;
            }
        } */
        
        iData.noOfRecords = readShort(in);
        return iData;
    }

    @Override
    public IData processLevel2Messages(BSEInputStream in) throws Exception {
        IData iData = new IData();
        skipBytes(in, 2);
        iData.scripCode = readInt(in);
        iData.scripId = iData.scripCode + "";
        iData.openPrice = readInt(in);
        iData.closePrice = readInt(in);
        iData.highPrice = readInt(in);
        iData.lowPrice = readInt(in);
        iData.noOfTrades = readInt(in);
        iData.tradedVolume = readInt(in);
        iData.tradedValue = readInt(in);
        iData.lastTradedQty = readInt(in);
        iData.lastTradedPrice = readInt(in);
        iData.totalBidQty = readInt(in);
        iData.totalSellQty = readInt(in);
        iData.tradeValueFlag = toChars(in, 1);
        iData.trend = toChars(in, 1);
        iData.sixLakhFlag = toChars(in, 1);
        skipBytes(in, 1);
        iData.lowerCircuit = readInt(in);
        iData.upperCircuit = readInt(in);
        iData.weightedAverage = readInt(in);
        int noOfRecords = 5;
        iData.mDepth06 = new int[5][4];
        for (int i = 0; i < noOfRecords; i++) {
            iData.mDepth06[i][0] = readInt(in);
            iData.mDepth06[i][1] = readInt(in);
            iData.mDepth06[i][2] = readInt(in);
            iData.mDepth06[i][3] = readInt(in);
        }
        return iData;
    }
}
