/*
 * to process the 1901 messagse type structure based on Binary OMB
 */
package com.intelym.streamer.messages.bse;

import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.data.IData;

/**
 *
 * @author Venkatesh
 */
public class MarketPicture1901 extends BroadcastMessages {

    public MarketPicture1901() {
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
        iData.highPrice = readInt(in); //BuyRate
        iData.totalBidQty = readInt(in); //buyQty
        iData.trend = toChars(in, 1);
        skipBytes(in, 3); //filler char(3)
        iData.lowPrice = readInt(in); //sellRate
        iData.totalSellQty = readInt(in); //sellQty
        iData.lastTradedPrice = readInt(in);
        iData.tradedVolume = readInt(in);
        return iData;
    }
}
