/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.curnse;

import com.intelym.streamer.communication.NSEInputStream;
import com.intelym.streamer.data.IData;

/**
 *
 * @author Hari Nair
 */
public class BroadcastMarketWatch {
    
    public BroadcastMarketWatch(){
    }
    
    public IData processLevel1Messages(NSEInputStream in) throws Exception{
        IData iData = new IData();
        iData.noOfRecords = in.readShort();
        return iData;
    }
    
    public IData processLevel2Messages(NSEInputStream in){
        IData iData = new IData();
        try{
            
            iData.scripCode = in.readInt();
            iData.bestBuyQty = in.readInt();
            iData.bestBuyPrice = in.readInt();
            iData.bestSellQty = in.readInt();
            iData.bestSellPrice = in.readInt();
            iData.lastTradedPrice = in.readInt();
            iData.lastTradedQty = in.readInt();
            iData.openInterest = in.readInt();
        }catch(Exception e){
            return null;
        }
        return iData;
    }
}
