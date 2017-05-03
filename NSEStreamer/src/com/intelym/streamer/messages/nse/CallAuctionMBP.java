/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.nse;

import com.intelym.streamer.common.CommonMessage;
import com.intelym.streamer.communication.NSEInputStream;
import com.intelym.streamer.data.IData;

/**
 *
 * @author HariNair
 */
public class CallAuctionMBP extends CommonMessage{
    
    public CallAuctionMBP(){
        
    }
    
    public IData processLevel1Messages(NSEInputStream in) throws Exception{
        IData iData = new IData();
        iData.noOfRecords = in.readShort();
        return iData;
    }
    
    public IData processLevel2Messages(NSEInputStream in){
        IData iData = new IData();
        try{
            iData.scripCode = in.readShort();
            iData.bookType = in.readShort();
            iData.tradingStatus = in.readShort();
            iData.tradedVolume = in.readInt();
            iData.lastTradedPrice = in.readInt();
            iData.trend = toChars(in, 1);
            iData.change = in.readInt();
            iData.lastTradedQty = in.readInt();
            iData.timeInMillis = in.readInt();
            iData.weightedAverage = in.readInt();
            
            // This line is added by Nirmal
            iData.timeStamp =sFddMMMYYYY.format(iData.timeInMillis);
            
            skipBytes(in, 4); // Skipping AuctionNumber, AuctionStatus, InitiatorType, InitiatorPrice, InitiatorQty, AuctionPrice, AuctionQuantity
            
            
            for(int i = 0; i < 5; i++){
                int quantity = in.readInt();
                int price = in.readInt();
                int numberOfOrders = in.readShort();
                int buySellFlag = in.readShort();
                iData.mDepth[i][0] = price;
                iData.mDepth[i][1] = quantity;
                iData.mDepth[i][4] = numberOfOrders;
            }
            for(int i = 0; i < 5; i++){
                int quantity = in.readInt();
                int price = in.readInt();
                int numberOfOrders = in.readInt();
                int buySellFlag = in.readInt();
                iData.mDepth[i][2] = price;
                iData.mDepth[i][3] = quantity;
                iData.mDepth[i][5] = numberOfOrders;
            }
            skipBytes(in, 4) ;
            iData.d_totalBidQty = in.readDouble();
            iData.d_totalSellQty = in.readDouble();
            skipBytes(in, 2);
            iData.closePrice = in.readInt();
            iData.openPrice = in.readInt();
            iData.highPrice = in.readInt();
            iData.lowPrice = in.readInt();
            //
        }catch(Exception e){}
    
        return iData;
    }
}
