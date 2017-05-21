/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.common;

/**
 *
 * @author Hari Nair
 */
public class Types {
    public static final int BSE_SESSION_CHANGE_BROADCAST = 1905,
                            BSE_AUCTION_CHANGE_BROADCAST = 10901,
                            BSE_TOUCHLINE_BROADCAST = 1901,
                            BSE_NEWSHEADLINE_BROADCAST = 1902,
                            BSE_OPENING_PRICE_BROADCAST = 1903,
                            BSE_CLOSING_PRICE_BROADCAST = 1904,
                            BSE_MARKETPICTURE_BROADCAST = 1906,
                            BSE_MARKETPICTURE_PCAS_BROADCAST = 2020,
                            BSE_TIME_BROADCAST = 1908,
                            BSE_VAR_PERCENT_BROADCAST = 4444,
                            BSE_SENSEX_DETAIL_BROADCAST = 1947,
                            BSE_ALLINDICES_BROADCAST = 1948,
                            BSE_OPEN_INTEREST_BROADCAST = 1949,
                            BSE_ENHANCED_BROADCAST = 1916,
                            BSE_AUNCTION_PICTURE_BROADCAST = 4141,
                            BSE_NFCAST_TIME = 2001,
                            BSE_NFCAST_SESSIONCHANGE = 2002,
                            BSE_NFCAST_AUCTION_CHANGE = 2003,
                            BSE_NFCAST_SENSEX_BROADCAST = 2011,
                            BSE_NFCAST_ALL_INDICES_BROACAST = 2012,
                            BSE_NFCAST_NEWSHEADERLINE = 2004,
                            BSE_NFCAST_OPEN = 2013,
                            BSE_NFCAST_CLOSE = 2014,
                            BSE_NFCAST_OI = 2015,
                            BSE_NFCAST_VAR = 2016,
                            BSE_NFCAST_AUCTION_MP = 2017,
            
                            NSE_MBO_MBP = 7200,
                            NSE_MKT_WATCH = 7201,
                            NSE_TICKER = 7202,
                            NSE_ONLY_MBP = 7208,
                            NSE_CALL_AUCTION_MBP = 7214,
                            NSE_BROADCAST_CALL_AUCTION = 7215,
                            NSE_BROADCAST_INDICES = 7207,
                            NSE_BROADCAST_INDICES_VIX = 7216,
                            NSE_ASSETPRICE_DETAILS = 7213,  
                            NSE_MBA_QUALITY_CHANGE = 7215,
                            MBA_PRICE_CHANGE = 7216,
                            NSE_SECURITY_UPDATE_INFO = 7305;
                                    
    public static final int BC_MARKETWATCH_NSET  = 1,
                            BC_MARKETWATCH_NSEM = 2,
                            BC_MARKETWATCH_BSET = 3, // only touchline
                            BC_MARKETWATCH_BSEM = 4, // detailed market picture
                            BC_MARKETDEPTH = 5,
                            BC_INDEX = 6,
            /*Added by venkatesh for no time and no depth */
                            BC_MARKETDEPTH_NOTIME_DEPTH = 7, // 5x4
                            BC_MARKETDEPTH_NOTIME_NODEPTH = 8, // for bse_touchline_broadcast
                            BC_MARKETDEPTH_NODEPTH_TIME = 9,
                            BC_DPR_CHANGE = 11;
    public static final int NSE = 0,
                            BSE = 1,
                            NCDEX = 4,
                            MCX = 5,
                            FONSE = 2,
                            CURNSE = 3,
                            INDEX = 11;
    
    public static final String  sRunningExchange_NSE = "NSE",
                                sRunningExchange_FONSE = "FONSE",
                                sRunningExchange_CURNSE = "CURNSE";
    
    public static final int iRunningExchange_NSE = 0,
                            iRunningExchange_FONSE = 2,
                            iRunningExchange_CURNSE = 3;
}
