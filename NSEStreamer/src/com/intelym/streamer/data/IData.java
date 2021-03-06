/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.data;

/**
 *
 * @author Hari Nair
 */
public class IData {
    public String timeStamp;
    public int tradingSession;
    public int noOfRecords;
    public int scripCode;
    public int exchange;
    public int highPrice;
    public int lowPrice;
    public int openPrice;
    public int closePrice;
    public int lastTradedPrice;
    public String scripId;
    public int publishCode;
    public int noOfTrades;
    public int tradedValue;
    public int tradedVolume;
    public int lastTradedQty;
    public int prevClosePrice;
    public int equilibriumPrice;
    public int equilibriumQty;
    public int totalBidQty;
    public int totalSellQty;
    public double d_totalBidQty;
    public double d_totalSellQty;
    public String tradeValueFlag;
    public String trend;
    public String sixLakhFlag;
    public int lowerCircuit;
    public int upperCircuit;
    public int weightedAverage;
    public short marketType;
    public short sessionNumber;
    public long timeInMillis;
    public int[][] mDepth = new int[5][6];
    public int[][] mDepth06 = null;
    public int[][] mDepth16 = null;
    public short bookType;
    public short tradingStatus;
    public int change;
    public int openInterest;
    public int bestBuyQty;
    public int bestBuyPrice;
    public int bestSellQty;
    public int bestSellPrice;
    
    @Override
    public String toString() {
        StringBuilder sBuilder = new StringBuilder();
        sBuilder.append("exchange:").append(exchange).append(",scripCode:").append(scripCode);
        
        return sBuilder.toString();
    }
}
