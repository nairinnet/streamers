/*
 * Heart of the App. Processes all the application messages
 */
package com.intelym.streamer.bse.process;

import com.intelym.logger.LoggerFactory;
import com.intelym.logger.QuickLogger;
import com.intelym.streamer.common.IndicesInfo;
import com.intelym.streamer.communication.BroadcastManager;
import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.streamer.data.IData;
import com.intelym.streamer.data.ScripData;
import com.intelym.streamer.headers.Header;
import com.intelym.streamer.headers.Types;
import com.intelym.streamer.messages.bse.IndicesBroadcast;
import com.intelym.streamer.messages.bse.IndicesBroadcast1948;
import com.intelym.streamer.messages.bse.MarketPicture1901;
import com.intelym.streamer.messages.bse.MarketPicture1906;
import com.intelym.streamer.messages.bse.MarketPictureBroadcast;
import com.intelym.streamer.messages.bse.OMBHeader;
import com.intelym.streamer.messages.bse.OpenInterestBroadcast;
import java.util.TreeMap;
import javolution.util.FastMap;

/**
 *
 * @author Hari Nair
 */
public class ProcessController implements ProcessManager {

    private static QuickLogger mLog = LoggerFactory.getLogger(ProcessController.class);
    private StreamerConfiguration eConfiguration = null;
    private BroadcastManager bManager = null;
    private Publisher aPublisher = null;
    private TreeMap<Integer, Long> scripsMap;
    private FastMap<Integer, ScripData> allScripsMap;
    private int differenceTime = 1000;
    private int lastAvailableSession = -1;

    public ProcessController(String config) {
        try {
            mLog.info("Reading streamer configuration data....");
            eConfiguration = StreamerConfiguration.newInstance(config);
            mLog.info("Streamer configuration reading is completed....");

            //TreeMap<String, IndicesInfo> indexMap;
            String tmp = eConfiguration.getString("RIWA.Streamer.DiffTimeout");
            if (tmp != null) {
                differenceTime = Integer.parseInt(tmp);
            }


            allScripsMap = new FastMap<Integer, ScripData>();
            aPublisher = new Publisher(eConfiguration, scripsMap);
        } catch (Exception lEx) {
            mLog.error("Processor Failed, Detailed Msg is : " + lEx.getMessage());
        }
    }

    public void startProcess() {
        try {
            mLog.info("Streamer Process is starting...........");
            String port = eConfiguration.getString("IML.ListenerPort");
            String address = eConfiguration.getString("IML.ListenerAddress");
            int sPort = new Integer(port).intValue();
            initiateBroadcastConnection(sPort, address);
            aPublisher.openConnection();
        } catch (Exception lEx) {
        }
    }

    private void initiateBroadcastConnection(int port, String addr) throws Exception {
        if (port < 0) {
            throw new Exception("Invalid broadcast parameters");
        }
        bManager = new BroadcastManager(port, addr, this);
        mLog.info("Broadcast pipe started.. waiting for messages...");
    }

    @Override
    public Header processHeader(BSEInputStream in) throws Exception {
        Header header = new OMBHeader();
        header.process(in);
        return header;
    }

    @Override
    public void processIncoming(BSEInputStream in, Header header) throws Exception {
        try {
            switch (header.messageCode) {
                case Types.BSE_MARKETPICTURE_BROADCAST:
                    getMarketPictureBroadcast1906(in);
                    break;
                case Types.BSE_MARKETPICTURE_PCAS_BROADCAST:
                    getMarketPictureBroadcast(in);
                    break;
                case Types.BSE_TOUCHLINE_BROADCAST:
                    getMarketPictureBroadcast1901(in);
                    break;
                case Types.BSE_ENHANCED_BROADCAST:
                    getMarketPictureBroadcast1916(in);
                    break;
                case Types.BSE_NFCAST_TIME:
                case Types.BSE_NFCAST_SESSIONCHANGE:
                case Types.BSE_NFCAST_AUCTION_CHANGE:
                    break;
                case Types.BSE_NFCAST_SENSEX_BROADCAST:
                case Types.BSE_NFCAST_ALL_INDICES_BROACAST:
                    getSensexBroadcast(in, Types.BSE_NFCAST_SENSEX_BROADCAST);
                    break;
                case Types.BSE_ALLINDICES_BROADCAST:
                case Types.BSE_SENSEX_DETAIL_BROADCAST:
                    getSensexBroadcast(in, Types.BSE_ALLINDICES_BROADCAST);
                    break;
                case Types.BSE_NFCAST_NEWSHEADERLINE:
                    break;
                case Types.BSE_NFCAST_OPEN:
                case Types.BSE_NFCAST_CLOSE:
                case Types.BSE_NFCAST_VAR:
                case Types.BSE_NFCAST_AUCTION_MP:
                    break;
                case Types.BSE_NFCAST_OI:
                    getOpenInterest(in, Types.BSE_NFCAST_OI);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            mLog.debug("Broadcast Interpretation Failed for MessageCode : " + header.messageCode + ", Detailed Msg is : " + e.getMessage());;
        }
    }

    @Override
    public void connectionAborted() {
    }

    /****** Message Area *************/
    private void getOpenInterest(BSEInputStream in, int flag) throws Exception {
        OpenInterestBroadcast oBroadcast = new OpenInterestBroadcast();
        IData iData = oBroadcast.processLevel1Messages(in);
        for (int i = 0; i < iData.noOfRecords; i++) {
             IData iData2 = oBroadcast.processLevel2Messages(in);
             iData2.publishCode = Types.BC_MARKETWATCH_BSET;
             iData2.timeStamp = iData.timeStamp;
             iData2.tradingSession = iData.tradingSession;
             streamToRiwa(iData2);
        }
    }
    
    private void getSensexBroadcast(BSEInputStream in, int flag) throws Exception {

        if (flag == Types.BSE_NFCAST_SENSEX_BROADCAST) {
            IndicesBroadcast iBroadcast = new IndicesBroadcast();
            IData iData = iBroadcast.processLevel1Messages(in);
            if (iBroadcast.isLevel2Available()) {
                for (int i = 0; i < iData.noOfRecords; i++) {
                    IData iData2 = iBroadcast.processLevel2Messages(in);
                    //iData2.timeStamp = iData.timeStamp;
                    iData2.timeInMillis = iData.timeInMillis;
                    iData2.tradingSession = iData.tradingSession;
                    if (lastAvailableSession > 4 && iData2.tradingSession == 4) {
                        continue;
                    }
                    lastAvailableSession = iData.tradingSession;
                    if (iData2.tradingSession == 0 || iData2.tradingSession == 7) {
                        continue;
                    }
                    if (iData2.lastTradedPrice <= 0) {
                        continue;
                    }
                    iData2.publishCode = Types.BC_INDEX;
                    streamToRiwa(iData2);
                }
            }
        } else if (flag == Types.BSE_ALLINDICES_BROADCAST) {
            IndicesBroadcast1948 iBroadcast = new IndicesBroadcast1948();
            IData iData = iBroadcast.processLevel1Messages(in);
            if (iBroadcast.isLevel2Available()) {
                for (int i = 0; i < iData.noOfRecords; i++) {
                    IData iData2 = iBroadcast.processLevel2Messages(in);
                    iData2.timeStamp = iData.timeStamp;
                    iData2.tradingSession = iData.tradingSession;
                    if (lastAvailableSession > 4 && iData2.tradingSession == 4) {
                        continue;
                    }
                    lastAvailableSession = iData.tradingSession;
                    if (iData2.tradingSession == 0 || iData2.tradingSession == 7) {
                        continue;
                    }
                    if (iData2.lastTradedPrice <= 0) {
                        continue;
                    }
                    iData2.publishCode = Types.BC_INDEX;
                    streamToRiwa(iData2);
                }
            }
        }
    }

    private void getMarketPictureBroadcast(BSEInputStream in) throws Exception {
        MarketPictureBroadcast mBroadcast = new MarketPictureBroadcast();
        IData iData = mBroadcast.processLevel1Messages(in);
        if (mBroadcast.isLevel2Available()) {
            for (int i = 0; i < iData.noOfRecords; i++) {
                IData iData2 = mBroadcast.processLevel2Messages(in);
                //iData2.timeStamp = iData.timeStamp;
                //iData2.tradingSession = iData.tradingSession;
                iData2.timeInMillis = iData.timeInMillis;
                iData2.publishCode = Types.BC_MARKETWATCH_BSEM;

                if (iData2.tradingSession == 0 || iData2.tradingSession == 7) {
                    continue;
                }
                if (iData2.lastTradedPrice <= 0) {
                    continue;
                }
                ScripData tmpScrip = allScripsMap.get(iData2.scripCode);
                if (differenceTime > 0) {
                    if (tmpScrip == null) {
                        ScripData sData = new ScripData();
                        sData.scripCode = iData2.scripCode;
                        sData.lastTradedPrice = iData2.lastTradedPrice;
                        sData.timeInMillis = iData2.timeInMillis;
                        sData.tradedVolume = iData2.tradedVolume;
                        allScripsMap.put(iData2.scripCode, sData);
                        streamToRiwa(iData2);
                    } else {
                        if (iData2.timeInMillis - tmpScrip.timeInMillis >= differenceTime) {
                            ScripData sData = new ScripData();
                            sData.scripCode = iData2.scripCode;
                            sData.lastTradedPrice = iData2.lastTradedPrice;
                            sData.timeInMillis = iData2.timeInMillis;
                            sData.tradedVolume = iData2.tradedVolume;
                            allScripsMap.put(iData2.scripCode, sData);
                            streamToRiwa(iData2);
                        } 
                        else {
                            //System.out.println("Dropping Packets for " + iData2.scripCode);
                            continue;
                        }
                    }
                } else {
                    streamToRiwa(iData2);
                }

            }
        }
    }

    /* No time for 1906 and  1901 */
    
    private void getMarketPictureBroadcast1906(BSEInputStream in) throws Exception {
        MarketPicture1906 mBroadcast = new MarketPicture1906();
        IData iData = mBroadcast.processLevel1Messages(in);
        if (mBroadcast.isLevel2Available()) {
            for (int i = 0; i < iData.noOfRecords; i++) {
                IData iData2 = mBroadcast.processLevel2Messages(in);
                iData2.publishCode = Types.BC_MARKETDEPTH_NOTIME_DEPTH; //without time and depth with 5x4
                
                if (iData2.tradingSession == 0 || iData2.tradingSession == 7) {
                    continue;
                }
                if (iData.lastTradedPrice <= 0) {
                    continue;
                }
                streamToRiwa(iData2);
            }
        }
    }
    
    private void getMarketPictureBroadcast1901(BSEInputStream in) throws Exception {
        MarketPicture1901 mBroadcast = new MarketPicture1901();
        IData iData = mBroadcast.processLevel1Messages(in);
        if (mBroadcast.isLevel2Available()) {
            for (int i = 0; i < iData.noOfRecords; i++) {
                IData iData2 = mBroadcast.processLevel2Messages(in);
                iData2.publishCode = Types.BC_MARKETDEPTH_NOTIME_NODEPTH; //without time and depth
                
                if (iData2.tradingSession == 0 || iData2.tradingSession == 7) {
                    continue;
                }
                if (iData.lastTradedPrice <= 0) {
                    continue;
                }
                streamToRiwa(iData2);
            }
        }
    }
    
    /* 1916 - No depth - Time Available */
     private void getMarketPictureBroadcast1916(BSEInputStream in) throws Exception {
        MarketPictureBroadcast mBroadcast = new MarketPictureBroadcast();
        IData iData = mBroadcast.processLevel1Messages(in);
        if (mBroadcast.isLevel2Available()) {
            for (int i = 0; i < iData.noOfRecords; i++) {
                IData iData2 = mBroadcast.processLevel2Messages(in);
                iData2.timeStamp = iData.timeStamp;
                //iData2.tradingSession = iData.tradingSession;
                iData2.timeInMillis = iData.timeInMillis;
                iData2.publishCode = Types.BC_MARKETDEPTH_NODEPTH_TIME;

                if (iData2.tradingSession == 0 || iData2.tradingSession == 7) {
                    continue;
                }
                if (iData.lastTradedPrice <= 0) {
                    continue;
                }
                ScripData tmpScrip = allScripsMap.get(iData2.scripCode);
                if (differenceTime > 0) {
                    if (tmpScrip == null) {
                        ScripData sData = new ScripData();
                        sData.scripCode = iData2.scripCode;
                        sData.lastTradedPrice = iData2.lastTradedPrice;
                        sData.timeInMillis = iData2.timeInMillis;
                        sData.tradedVolume = iData2.tradedVolume;
                        allScripsMap.put(iData2.scripCode, sData);
                        streamToRiwa(iData2);
                    } else {
                        if (iData2.timeInMillis - tmpScrip.timeInMillis >= differenceTime) {
                            ScripData sData = new ScripData();
                            sData.scripCode = iData2.scripCode;
                            sData.lastTradedPrice = iData2.lastTradedPrice;
                            sData.timeInMillis = iData2.timeInMillis;
                            sData.tradedVolume = iData2.tradedVolume;
                            allScripsMap.put(iData2.scripCode, sData);
                            streamToRiwa(iData2);
                        } /*else if(tmpScrip.tradedVolume != iData2.tradedVolume){
                        ScripData sData = new ScripData();
                        sData.scripCode = iData2.scripCode;
                        sData.lastTradedPrice = iData2.lastTradedPrice;
                        sData.timeInMillis = iData2.timeInMillis;
                        sData.tradedVolume = iData2.tradedVolume;
                        allScripsMap.put(iData2.scripCode, sData);
                        streamToRiwa(iData2);
                        }*/ else {
                            //System.out.println("Dropping Packets for " + iData2.scripCode);
                            continue;
                        }
                    }
                } else {
                    streamToRiwa(iData2);
                }

            }
        }
    }

    private void streamToRiwa(IData iData) {
        aPublisher.add(iData);
    }
}
