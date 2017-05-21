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
public class BroadcastTicker extends CommonMessage{
    
    public BroadcastTicker(){
        
    }
    
    public IData processLevel1Messages(NSEInputStream in, Header header) throws Exception{
        IData iData = new IData();
        iData.noOfRecords = in.readShort();
        return iData;
    }
    
    public IData processLevel2Messages(NSEInputStream in, Header header) throws Exception{
        IData iData = new IData();
        iData.scripCode = in.readInt();
        iData.marketType = in.readShort();
        iData.lastTradedPrice = in.readInt();
        iData.tradedVolume = in.readInt();
        iData.openInterest = in.readInt();
        iData.highPrice = in.readInt();
        iData.lowPrice = in.readInt();
        return iData;
    }
}

