/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.fonse;

import com.intelym.streamer.common.CommonMessage;
import com.intelym.streamer.communication.NSEInputStream;
import com.intelym.streamer.data.IData;

/**
 *
 * @author HariNair
 */
public class BroadcastMBOMBP extends CommonMessage{
    
    public BroadcastMBOMBP(){
        
    }
    
    public void processLevel1Messages(NSEInputStream in){
        
    }
    
    public IData processLevel2Messages(NSEInputStream in){
        IData iData = new IData();
        try{
            
            iData.scripCode = in.readInt();
            iData.bookType = in.readShort();
            iData.tradingStatus = in.readShort();
            iData.tradedVolume = in.readInt();
            iData.lastTradedPrice = in.readInt();
            iData.trend = toChars(in, 2);
            iData.change = in.readInt();
            iData.lastTradedQty = in.readInt();
            iData.timeInMillis = in.readInt();
            iData.weightedAverage = in.readInt();
            
            // This line is added by Nirmal
            iData.timeStamp =sFddMMMYYYY.format(iData.timeInMillis);
            
            int auctionNumber = in.readShort();
            int auctionStatus = in.readShort();
            int initiatorType = in.readShort();
            int initiatorPrice = in.readInt();
            int initiatorQty = in.readInt();
            int auctionPrice = in.readInt();
            int auctionQty = in.readInt();
            //Reading MBO Information
            //Actually Skipping it
            skipBytes(in, 180);
            
            //Reading MBP Information
            //12 * 10 - 5 for Buy and 5 for Sell
            
            for(int i = 0; i < 5; i++){
                int quantity = in.readInt();
                int price = in.readInt();
                int numberOfOrders = in.readShort();
                iData.mDepth[i][0] = price;
                iData.mDepth[i][1] = quantity;
                iData.mDepth[i][4] = numberOfOrders;
            }
            for(int i = 0; i < 5; i++){
                int quantity = in.readInt();
                int price = in.readInt();
                int numberOfOrders = in.readShort();
                iData.mDepth[i][2] = price;
                iData.mDepth[i][3] = quantity;
                iData.mDepth[i][5] = numberOfOrders;
            }
            iData.d_totalBidQty = in.readDouble();
            iData.d_totalSellQty = in.readDouble();
            in.readByte();
            in.readByte();
            iData.closePrice = in.readInt();
            iData.openPrice = in.readInt();
            iData.highPrice = in.readInt();
            iData.lowPrice = in.readInt();
            //
        }catch(Exception e){}
    
        return iData;
    }
}
