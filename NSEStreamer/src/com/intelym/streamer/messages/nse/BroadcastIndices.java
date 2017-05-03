/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.nse;

import com.intelym.streamer.common.CommonMessage;
import com.intelym.streamer.common.Header;
import com.intelym.streamer.communication.NSEInputStream;
import com.intelym.streamer.data.IData;

/**
 *
 * @author Hari Nair
 */
public class BroadcastIndices extends CommonMessage{
    
    public BroadcastIndices(){
        
    }
    
    public IData processLevel1Messages(NSEInputStream in) throws Exception{
        IData iData = new IData();
        iData.noOfRecords = in.readShort();
        return iData;
        
    }
    
    public IData processLevel2Messages(NSEInputStream in) throws Exception{
        IData iData = new IData();
        iData.scripId = toChars(in, 22).trim();
        iData.lastTradedPrice = in.readInt();
        iData.highPrice = in.readInt();
        iData.lowPrice = in.readInt();
        iData.openPrice = in.readInt();
        iData.closePrice = in.readInt();
        iData.change = in.readInt();
        skipBytes(in, 24);
        iData.trend = toChars(in, 2);
        skipBytes(in, 1);
        return iData;
    }
}
