/*
 * Publishes the broadcast to Riwa on IGMP or Unicast Broadcast
 */
package com.intelym.streamer.nse.process;

import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;
import com.intelym.streamer.common.Constants;
import com.intelym.streamer.common.IndicesInfo;
import com.intelym.streamer.common.Types;
import com.intelym.streamer.communication.NSEOutputStream;
import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.streamer.data.IData;
import com.intelym.streamer.data.ScripData;
import com.intelym.streamer.sql.DerivativesScrip;
import com.intelym.streamer.sql.SQLUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Hari Nair
 */
public class Publisher implements Runnable {

    private static final RiwaLogger mLog = LoggerFactory.getLogger(Publisher.class);
    private StreamerConfiguration sConfiguration;
    private MulticastSocket mSocket, mAsciiSocket;
    private MulticastSocket mSocket1;
    private int sPort = 7000, sAsciiPort = 7001;
    private int sPort1 = 0;
    private InetAddress sAddress = null, sAsciiAddress = null;
    private InetAddress sBindAddress = null, sAsciiBindAddress = null;
    private boolean sMulticast = false;
    private boolean isRunning = false;
    private Thread streamThread = null;
    private long delayTimeDifference;
    private int outputType = 1; //1 is TCP and 2 is WS       
    private final LinkedBlockingQueue<IData> publisherQueue;
    private HashMap<String, IndicesInfo> indexMap = null;
    private final TreeMap<Integer, ScripData> scripsMap;
    private final TreeMap<Integer, ScripData> scripFeedTimeBased; // Modified by Nirmal
    private final TreeMap<String, ArrayList> scripMapTimeBased; // Modified by Nirmal
    private final TreeMap<Integer, IData> dprChangeMap;
    private NumberFormat nF;
    private SQLUtils sqlUtils = null;
    private HashMap<Integer, DerivativesScrip> derivativeMappingScrips = null;
    private int runningExchange = -1;
    private IndexLoader indexLoader = null;
    private boolean isAsciiRequired = false;
    
    private boolean isAdditionalOutputEnabled = false;
    private List<AdditionalIps> additionalIps = null;

    public Publisher(StreamerConfiguration aConfiguration, TreeMap sMap, TreeMap iMap, TreeMap sTimeBasedScrip) { // Modified by Nirmal
        scripsMap = sMap;
        scripMapTimeBased = sTimeBasedScrip; // Modified by Nirmal
        scripFeedTimeBased = new TreeMap<>(); // Modified by Nirmal
        sConfiguration = aConfiguration;
        publisherQueue = new LinkedBlockingQueue();
        indexLoader = new IndexLoader();
        dprChangeMap = new TreeMap<>();
        nF = NumberFormat.getInstance();
        nF.setMinimumFractionDigits(2);
        nF.setMaximumFractionDigits(2);
        nF.setGroupingUsed(false);
        try{
            indexMap = indexLoader.getIndexMap();
            if(sConfiguration.getString("RIWA.Exchange") != null) {
                if(sConfiguration.getString("RIWA.Exchange").equalsIgnoreCase(Types.sRunningExchange_FONSE) || sConfiguration.getString("RIWA.Exchange").equalsIgnoreCase(Types.sRunningExchange_CURNSE)) {
                    sqlUtils = new SQLUtils();
                    if (sConfiguration.getString(Constants.CSV_FILE).equalsIgnoreCase("NO")) {
                        derivativeMappingScrips = sqlUtils.getObjectList(indexMap);
                    } else {
                        derivativeMappingScrips = sqlUtils.getObjectListFromFile(indexMap);
                    }
                    
                }
            }
            
        }catch(Exception e){
            mLog.info("Unable to connect to database to load derivative scrips");
                    
        }
        
        String tmp = sConfiguration.getString("RIWA.Output.Type");
        if (tmp != null) {
            switch (tmp) {
                case "TCP":
                    outputType = Constants.TCP;
                    break;
                case "WS":
                    outputType = Constants.WEBSOCKET;
                    break;
                default:
                    mLog.error("Invalid output type for stream broadcast.. given " + tmp + ", expected TCP or WS");
                    System.exit(0);
            }
        }
        tmp = sConfiguration.getString("RIWA.Exchange");
        if(tmp != null){
            if(tmp.equalsIgnoreCase(Types.sRunningExchange_NSE)){
                runningExchange = Types.iRunningExchange_NSE;
            }
            else if(tmp.equalsIgnoreCase(Types.sRunningExchange_FONSE)){
                runningExchange = Types.iRunningExchange_FONSE;
            }
            else if(tmp.equalsIgnoreCase(Types.sRunningExchange_CURNSE)){
                runningExchange = Types.iRunningExchange_CURNSE;
            }
            else{
                mLog.error("No Defined Exchange for this application, provide either NSE/FONSE/CURNSE");
                System.exit(0);
            }
        }
        
        try{
        tmp = sConfiguration.getString("RIWA.AdditionalOutputs");
        if(tmp.equalsIgnoreCase("true")){
            this.isAdditionalOutputEnabled = true;
            tmp = sConfiguration.getString("RIWA.AdditionalOutputAddress");
            String[] tmpArray = tmp.split("\\,");
            additionalIps = new ArrayList<>();
            for(String ips : tmpArray){
                String[] ipsAndPorts = ips.split("\\:");
                AdditionalIps aIps = new AdditionalIps();
                aIps.additionalIp = InetAddress.getByName(ipsAndPorts[0].trim());
                aIps.additionalPort = Integer.valueOf(ipsAndPorts[1].trim());
                additionalIps.add(aIps);
            }
        }
        }catch(Exception e){
            mLog.error("Unable to add additional ips for multiple outputs " + e.getMessage());
            
        }
    }

    

    private IndicesInfo getIndex(int id) {
        return new IndicesInfo(id);
    }

   
    /**
     * Opens the connection to stream socket
     * @return 
     */
    public boolean openConnection() {
        try {
            String timeDifference = sConfiguration.getString("RIWA.Streamer.TimeDifference");
            if (timeDifference == null) {
                delayTimeDifference = 2000;
            } else {
                delayTimeDifference = Long.parseLong(timeDifference);
            }
            String port = sConfiguration.getString("RIWA.StreamPort");
            String port1 = sConfiguration.getString("RIWA.StreamPort1");
            String address = sConfiguration.getString("RIWA.StreamAddress");
            String isMulticast = sConfiguration.getString("RIWA.StreamInMulticast");
            String bindAddress = sConfiguration.getString("RIWA.StreamBindAddress");
            if (port == null || address == null) {
                mLog.error("Invalid RIWA.Stream Information, Check Configuration ...");
                return false;
            }
            sPort = new Integer(port);
            sAddress = InetAddress.getByName(address);
            if (isMulticast != null) {
                sMulticast = Boolean.valueOf(isMulticast);
            }
            if (bindAddress != null) {
                sBindAddress = InetAddress.getByName(bindAddress);
            }
            mSocket = new MulticastSocket();

            streamThread = new Thread(this, "RIWA.StreamThread");

            if (port1 != null & !port1.equals("")) {
                sPort1 = Integer.parseInt(port1);
            }

            if (sPort1 > 0) {
                mSocket1 = new MulticastSocket();
            }
            String tmp = sConfiguration.getString("RIWA.AsciiOutputEnabled");
            if (tmp != null && tmp.equalsIgnoreCase("true")){
                isAsciiRequired = true;
                tmp = sConfiguration.getString("RIWA.AsciiOutputAddress");
                sAsciiAddress = InetAddress.getByName(tmp);
                tmp = sConfiguration.getString("RIWA.AsciiStreamPort");
                sAsciiPort = Integer.parseInt(tmp);
                bindAddress = sConfiguration.getString("RIWA.AsciiStreamBindAddress");
                if (bindAddress != null) {
                    sAsciiBindAddress = InetAddress.getByName(bindAddress);
                }
                mAsciiSocket = new MulticastSocket();
            }
            
            isRunning = true;
            streamThread.start();
            
            
            mLog.info("RIWA.Stream queue started successfully....");
            
        } catch (IOException | NumberFormatException e) {
            mLog.error("RIWA Publisher Died, Detailed Msg is : " + e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Adds the data into Queue, which is read and cleared by the publisher
     * @param iData 
     */
    public void add(IData iData) {
        try {
            if (publisherQueue.size() > 1000) {
                publisherQueue.clear();
            }
            publisherQueue.add(iData);

        } catch (Exception e) {
            mLog.error("Problem in RIWA.Stream queue, Detailed info : " + e.getLocalizedMessage());
        }
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                //try {
                    //IData iData = publisherQueue.poll(100l, TimeUnit.NANOSECONDS);
                    IData iData = publisherQueue.poll(10, TimeUnit.MILLISECONDS);
                    //IData iData = publisherQueue.poll();
                    if (iData != null) {
                        publishData(iData);
                    }
                    // publisherQueue.wait();
//                } catch (InterruptedException e) {
//                }
            }
        } catch (Exception e) {
            isRunning = false;
        }
    }

    public void publishData(IData iData) {
        String data = null;
        String mDepthData = null;
        byte[] buffer, mDepthBuffer;
        try {
            switch (outputType) {
                case Constants.WEBSOCKET:
                    switch (iData.publishCode) {
                        case Types.BC_INDEX:
                            data = sendIndexBroadcast_WS(iData);
                            if (data != null) {
                                stream(data);
                            }
                            break;
                        case Types.BC_MARKETWATCH_NSEM:
                            data = sendMarketPictureBroadcast_WS(iData);
                            mDepthData = sendMDepthDataOverBinary(iData);
                            if (data != null) {
                                stream(data);
                            }
                            if (mDepthData != null) {
                                stream(mDepthData);
                            }
                            break;
                        case Types.BC_MARKETWATCH_BSET:
                            break;
                        case Types.BC_MARKETWATCH_NSET:
                            data = sendOpenInterest(iData);
                            if(data != null){
                                stream(data);
                            }
                            break;
                        case Types.BC_MARKETDEPTH:
                            break;
                        case Types.BC_MARKETDEPTH_NOTIME_NODEPTH:
                            data = sendMarketPictureBroadcast1901(iData);
                            if (data != null) {
                                stream(data);
                            }
                            break;
                        case Types.BC_MARKETDEPTH_NOTIME_DEPTH:
                            data = sendMarketPictureBroadcast1906(iData);
                            if (data != null) {
                                stream(data);
                            }
                            break;
                         case Types.BC_MARKETDEPTH_NODEPTH_TIME:
                            data = sendMarketPictureBroadcast1916(iData);
                            if (data != null) {
                                stream(data);
                            }
                            break;
                    }
                    break;
                case Constants.TCP:
                    switch (iData.publishCode) {
                        case Types.BC_INDEX:
                            buffer = sendIndexBroadcast_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;
                        case Types.BC_MARKETWATCH_NSEM:
                            //mLog.info("iData : " + iData.scripCode);
                            if(iData.lastTradedPrice == 0 || (iData.mDepth[0][0] == 0 && iData.mDepth[0][2] == 0)) {
                                //mLog.info("iData.scripCode : " + iData.scripCode + " BestBuyPrice:mDepth[0][0] : " + iData.mDepth[0][0] + " BestSellPrice:iData.mDepth[0][2] : " + iData.mDepth[0][2] + " BestBuyQty:mDepth[0][1] : " + iData.mDepth[0][1]  + " BestSellQty:iData.mDepth[0][3] : " + iData.mDepth[0][3] + " iData.lastTradedPrice : " + iData.lastTradedPrice);
                                break;
                            }
                            
                            buffer = sendMarketPictureBroadcast_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            mDepthBuffer = sendMDepthDataOverBinary_TCP(iData);
                            if (mDepthBuffer != null) {
                                streamRawbuffer(mDepthBuffer);
                            }
                            break;
                        case Types.BC_MARKETDEPTH_NOTIME_DEPTH:
                            buffer = sendMarketPictureBroadcast1906_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;
                        case Types.BC_MARKETDEPTH_NODEPTH_TIME:
                            buffer = sendMarketPictureBroadcast1916_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;

                        case Types.BC_MARKETDEPTH_NOTIME_NODEPTH:
                            buffer = sendMarketPictureBroadcast1901_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;
                        case Types.BC_MARKETWATCH_NSET:
                            buffer = sendOpenInterest_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;
                        case Types.BC_MARKETWATCH_BSET:
                            break;
                        case Types.BC_MARKETDEPTH:
                            break;
                        case Types.BC_DPR_CHANGE:
                            buffer = sendDPRChange_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;
                            
                    }
                    break;
            }

        } catch (Exception lEx) {
            mLog.error("Publish to RIWA Fails, Detailed Msg is : " + lEx.getMessage());
        }
    }

    /* NO time - Depth */
    private String sendMarketPictureBroadcast1906(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (scripsMap != null) {
            if (scripsMap.containsKey(iData.scripCode)) {
                ScripData scripData = scripsMap.get(iData.scripCode);
                scripData.lastTradedPrice = iData.lastTradedPrice;
                scripData.prevClosePrice = iData.prevClosePrice;
                scripsMap.put(iData.scripCode, scripData);
            }
        }
        if (doBroadcast) {
            //type
            
            String LTT = " ";
            try{
            DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            long milliSeconds= iData.timeInMillis;
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(milliSeconds);
            LTT = formatter.format(calendar.getTime());
            }catch(Exception ex){}
            
            
            int exchangeCode = Types.NSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            NSEOutputStream dataOut = new NSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(1);
            int length = iData.scripId.length();
            dataOut.writeByte(length);

            dataOut.writeBytes(iData.scripId);

            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.prevClosePrice);
            dataOut.writeInt(iData.tradedValue);
            dataOut.writeInt(iData.mDepth06[0][0]); //BestBuyRate
            dataOut.writeInt(iData.mDepth06[0][1]); //TotalBuyQty
            dataOut.writeInt(iData.mDepth06[0][2]); //BestSellRate
            dataOut.writeInt(iData.mDepth06[0][3]); //TotSellQty
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
            dataOut.writeInt(iData.lastTradedQty);
            dataOut.writeInt(iData.weightedAverage);
            dataOut.writeInt(iData.totalBidQty);
            dataOut.writeInt(iData.totalSellQty);
            // Next 3 Lines :  Copied by Nirmal - instuction by Hari on 05-01-2014
            dataOut.writeInt(iData.lowerCircuit);
            dataOut.writeInt(iData.upperCircuit);
            dataOut.writeByte(LTT.length());
            dataOut.writeBytes(LTT);
            dataOut.writeByte(Integer.valueOf(iData.tradingSession).byteValue());
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;
        } else {
            return null;
        }
    }

    private byte[] sendDPRChange_TCP(IData iData) throws Exception {
        dprChangeMap.put(iData.scripCode, iData);
        int exchangeCode = runningExchange;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        NSEOutputStream dataOut = new NSEOutputStream(byteArray);
        dataOut.writeByte(1);
        dataOut.writeByte(exchangeCode);
        dataOut.writeByte(21);
        int length = String.valueOf(iData.scripCode).length();
        //length of this packet top to bottom
        // entire packet + 1 byte length scripcode + length of the scripcode 
        dataOut.writeByte(12 + 1 + length); 
        dataOut.writeByte(length);
        dataOut.writeBytes(String.valueOf(iData.scripCode));
        dataOut.writeInt(iData.lowerCircuit);
        dataOut.writeInt(iData.upperCircuit);
        return byteArray.toByteArray();
    }
    
    private byte[] sendMarketPictureBroadcast1906_TCP(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (dprChangeMap.containsKey(iData.scripCode)){
            IData iData2 = dprChangeMap.get(iData.scripCode);
            iData.lowerCircuit = iData2.lowerCircuit;
            iData.upperCircuit = iData2.upperCircuit;
        }
        if (scripsMap != null) {
            if (scripsMap.containsKey(iData.scripCode)) {
                ScripData scripData = scripsMap.get(iData.scripCode);
                scripData.lastTradedPrice = iData.lastTradedPrice;
                scripData.prevClosePrice = iData.prevClosePrice;
                scripsMap.put(iData.scripCode, scripData);
            }
        }
        if (doBroadcast) {
            //type
            
            String LTT = " ";
            switch(runningExchange){
                
                case Types.iRunningExchange_NSE:
                case Types.iRunningExchange_CURNSE:
                {
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(1);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(1);
                    int length = String.valueOf(iData.scripCode).length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(76 + 1 + length); 
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeInt(iData.lastTradedPrice);

                    // Next 2 to lines needs to be approved by HARI 
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.mDepth[0][0]);
                    dataOut.writeInt(iData.mDepth[0][1]);
                    dataOut.writeInt(iData.mDepth[0][2]);
                    dataOut.writeInt(iData.mDepth[0][3]);
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.lastTradedQty);
                    dataOut.writeInt(iData.weightedAverage);
                    dataOut.writeInt(Double.valueOf(iData.d_totalBidQty).intValue());
                    dataOut.writeInt(Double.valueOf(iData.d_totalSellQty).intValue());
                    dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeLong(iData.timeInMillis);
                    if (isAsciiRequired){
                        String asciiHeader = "1^15MIN^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^1^4^1^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "N^^" + format(iData.lastTradedPrice) + "^" + format(iData.mDepth[0][0]) + "^";
                        asciiData += "^" + iData.mDepth[0][1] + "^" + format(iData.mDepth[0][2])  + "^" + iData.mDepth[0][3] + "^";
                        asciiData += format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.openPrice) + "^";
                        asciiData += format(iData.closePrice) + "^" + iData.d_totalBidQty  + "^" + iData.d_totalSellQty + "^";
                        asciiData += iData.lastTradedQty + "^" + iData.tradedVolume + "^" + iData.tradedValue + "^";
                        asciiData += "^^^^" + format(iData.weightedAverage) + "^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                case Types.iRunningExchange_FONSE:
                {
                    DerivativesScrip dScrip = derivativeMappingScrips.get(iData.scripCode);
                    if (dScrip == null){
                        return null;
                    }
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(111);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(5);
                    
                    int length = String.valueOf(iData.scripCode).length();
                    int usLength = dScrip.cmToken.length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(66 + 1 + length + 1 + usLength); 
                    
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeByte(usLength);
                    dataOut.writeBytes(dScrip.cmToken);
                    dataOut.writeByte(dScrip.isFuture ? 1 : 0);
                    dataOut.writeInt(iData.lastTradedPrice);
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.mDepth[0][0]);
                    dataOut.writeInt(iData.mDepth[0][1]);
                    dataOut.writeInt(iData.mDepth[0][2]);
                    dataOut.writeInt(iData.mDepth[0][3]);
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.lastTradedQty);
                    dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeLong(iData.timeInMillis);
                    dataOut.writeByte(dScrip.isIndex ? 1 : 0);
                    if (isAsciiRequired){
                        String asciiHeader = "1^LIVE^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^11^4^5^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "^^^^^" + format(iData.openPrice) + "^" + format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.closePrice) + "^" + format(iData.lastTradedPrice) + "^^^" + iData.tradedVolume + "^" + iData.tradedValue + "^^^" + format(iData.mDepth[0][0]) + "^";
                        asciiData += "^" + iData.mDepth[0][2] + "^" + format(iData.mDepth[0][1])  + "^" + iData.mDepth[0][3] + "^^^" + iData.lastTradedQty + "^^^" + format(iData.weightedAverage) + "^" +  iData.totalBidQty  + "^" + iData.totalSellQty + "^^^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                default:
                    return null;
            }
            
        } else {
            return null;
        }
    }
    
    private String sendMarketPictureBroadcast1916(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (scripsMap != null) {
            if (scripsMap.containsKey(iData.scripCode)) {
                ScripData scripData = scripsMap.get(iData.scripCode);
                long timeInMillis = scripData.timeInMillis;
                scripData.timeInMillis = iData.timeInMillis;
                scripData.lastTradedPrice = iData.lastTradedPrice;
                scripData.prevClosePrice = iData.prevClosePrice;
                if (timeInMillis != 0 & delayTimeDifference > 0) {
                    if ((iData.timeInMillis - timeInMillis) >= delayTimeDifference) {
                        scripsMap.put(iData.scripCode, scripData);
                        doBroadcast = true;
                    } else {
                        doBroadcast = false;
                    }
                } else {
                    scripsMap.put(iData.scripCode, scripData);
                }

            }
        }
        if (doBroadcast) {
            //For BSE Packet Type 3
            // Len(ScripId)^ScripId^LTP^TTQ^TTV^High^Low^Open^Close^LTQ^Avg^TotalBidQty^TotalSellQty^Len(timeStamp)^TimeStamp^TradingSession
            
            String LTT = " ";
            try{
            DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            long milliSeconds= iData.timeInMillis;
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(milliSeconds);
            LTT = formatter.format(calendar.getTime());
            }catch(Exception ex){}
            
            
            int exchangeCode = Types.NSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            NSEOutputStream dataOut = new NSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(3);
            int length = iData.scripId.length();
            dataOut.writeByte(length);
            dataOut.writeBytes(iData.scripId);
            dataOut.writeInt(iData.lastTradedPrice);
            
            dataOut.writeInt(iData.tradedValue);
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
            dataOut.writeInt(iData.closePrice);
            dataOut.writeInt(iData.lastTradedQty);
            dataOut.writeInt(iData.weightedAverage);
            dataOut.writeInt(iData.totalBidQty);
            dataOut.writeInt(iData.totalSellQty);
            dataOut.writeByte(LTT.length());
            dataOut.writeBytes(LTT);
            dataOut.writeByte(Integer.valueOf(iData.tradingSession).byteValue());
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;
        } else {
            return null;
        }
    }
    
    private byte[] sendMarketPictureBroadcast1916_TCP(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (dprChangeMap.containsKey(iData.scripCode)){
            IData iData2 = dprChangeMap.get(iData.scripCode);
            iData.lowerCircuit = iData2.lowerCircuit;
            iData.upperCircuit = iData2.upperCircuit;
        }
        if (scripsMap != null) {
            if (scripsMap.containsKey(iData.scripCode)) {
                ScripData scripData = scripsMap.get(iData.scripCode);
                long timeInMillis = scripData.timeInMillis;
                scripData.timeInMillis = iData.timeInMillis;
                scripData.lastTradedPrice = iData.lastTradedPrice;
                scripData.prevClosePrice = iData.prevClosePrice;
                if (timeInMillis != 0 & delayTimeDifference > 0) {
                    if ((iData.timeInMillis - timeInMillis) >= delayTimeDifference) {
                        scripsMap.put(iData.scripCode, scripData);
                        doBroadcast = true;
                    } else {
                        doBroadcast = false;
                    }
                } else {
                    scripsMap.put(iData.scripCode, scripData);
                }

            }
        }
        if (doBroadcast) {
            //For BSE Packet Type 3
            // Len(ScripId)^ScripId^LTP^TTQ^TTV^High^Low^Open^Close^LTQ^Avg^TotalBidQty^TotalSellQty^Len(timeStamp)^TimeStamp^TradingSession
            
            String LTT = " ";
             switch(runningExchange){
                
                case Types.iRunningExchange_NSE:
                case Types.iRunningExchange_CURNSE:
                {
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(1);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(13);
                    int length = String.valueOf(iData.scripCode).length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(60 + 1 + length); 
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeInt(iData.scripCode);
                    dataOut.writeInt(iData.lastTradedPrice);
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.lastTradedQty);
                    dataOut.writeInt(iData.weightedAverage);
                    dataOut.writeInt(iData.totalBidQty);
                    dataOut.writeInt(iData.totalSellQty);
                    dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeLong(iData.timeInMillis);
                    if (isAsciiRequired){
                        String asciiHeader = "1^15MIN^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^1^4^1^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "N^^" + format(iData.lastTradedPrice) + "^^^^^^";
                        asciiData += format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.openPrice) + "^";
                        asciiData += format(iData.closePrice) + "^" + iData.d_totalBidQty  + "^" + iData.d_totalSellQty + "^";
                        asciiData += iData.lastTradedQty + "^" + iData.tradedVolume + "^" + iData.tradedValue + "^";
                        asciiData += "^^^^" + format(iData.weightedAverage) + "^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                case Types.iRunningExchange_FONSE:
                {
                    DerivativesScrip dScrip = derivativeMappingScrips.get(iData.scripCode);
                    if (dScrip == null){
                        return null;
                    }
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(111);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(5);
                    
                    int length = String.valueOf(iData.scripCode).length();
                    int usLength = dScrip.cmToken.length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(66 + 1 + length + 1 + usLength); 
                    
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeByte(usLength);
                    dataOut.writeBytes(dScrip.cmToken);
                    dataOut.writeByte(dScrip.isFuture ? 1 : 0);
                    dataOut.writeInt(iData.lastTradedPrice);
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.mDepth[0][0]);
                    dataOut.writeInt(iData.mDepth[0][1]);
                    dataOut.writeInt(iData.mDepth[0][2]);
                    dataOut.writeInt(iData.mDepth[0][3]);
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.lastTradedQty);
                    dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeLong(iData.timeInMillis);
                    dataOut.writeByte(dScrip.isIndex ? 1 : 0);
                    if (isAsciiRequired){
                        String asciiHeader = "1^LIVE^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^11^4^5^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "^^^^^" + format(iData.openPrice) + "^" + format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.closePrice) + "^" + format(iData.lastTradedPrice) + "^^^" + iData.tradedVolume + "^" + iData.tradedValue + "^^^" + format(iData.mDepth[0][0]) + "^";
                        asciiData += "^" + iData.mDepth[0][2] + "^" + format(iData.mDepth[0][1])  + "^" + iData.mDepth[0][3] + "^^^" + iData.lastTradedQty + "^^^" + format(iData.weightedAverage) + "^" +  iData.totalBidQty  + "^" + iData.totalSellQty + "^^^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                default:
                    return null;
             }
        } else {
            return null;
        }
    }
    
    /* No time - No Depth */
    private String sendMarketPictureBroadcast1901(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (scripsMap != null) {
            if (scripsMap.containsKey(iData.scripCode)) {
                ScripData scripData = scripsMap.get(iData.scripCode);
                scripData.lastTradedPrice = iData.lastTradedPrice;
                scripsMap.put(iData.scripCode, scripData);
            }
        }
        if (doBroadcast) {
            //For BSE Packet Type 2
            // Len(ScripId)^ScripId^LTP^Buy Rate^BuyQty^Sell Rate^Sell Qty^TTV
            
            int exchangeCode = Types.NSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            NSEOutputStream dataOut = new NSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(2);
            int length = iData.scripId.length();
            dataOut.writeByte(length);
            dataOut.writeBytes(iData.scripId);
            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.highPrice); // Buy Rate
            dataOut.writeInt(iData.totalBidQty); //Buy Qty
            dataOut.writeInt(iData.lowPrice); //Sell Price
            dataOut.writeInt(iData.totalSellQty); // Sell Qty
            dataOut.writeInt(iData.tradedVolume);
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;
        } else {
            return null;
        }
    }

    private byte[] sendMarketPictureBroadcast1901_TCP(IData iData) throws Exception {
        boolean doBroadcast = true;
         if (dprChangeMap.containsKey(iData.scripCode)){
            IData iData2 = dprChangeMap.get(iData.scripCode);
            iData.lowerCircuit = iData2.lowerCircuit;
            iData.upperCircuit = iData2.upperCircuit;
        }
        if (scripsMap != null) {
            if (scripsMap.containsKey(iData.scripCode)) {
                ScripData scripData = scripsMap.get(iData.scripCode);
                scripData.lastTradedPrice = iData.lastTradedPrice;
                scripsMap.put(iData.scripCode, scripData);
            }
        }
        if (doBroadcast) {
            //For BSE Packet Type 2
            // Len(ScripId)^ScripId^LTP^Buy Rate^BuyQty^Sell Rate^Sell Qty^TTV
            if (runningExchange == Types.iRunningExchange_NSE || runningExchange == Types.iRunningExchange_CURNSE){
                int exchangeCode = runningExchange;
                ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                dataOut.writeByte(1);
                dataOut.writeByte(exchangeCode);
                dataOut.writeByte(2);
                int length = String.valueOf(iData.scripCode).length();
                //length of this packet top to bottom
                // entire packet + 1 byte length scripcode + length of the scripcode 
                dataOut.writeByte(44 + 1 + length); 
                dataOut.writeByte(length);
                dataOut.writeBytes(String.valueOf(iData.scripCode));
                dataOut.writeInt(iData.lastTradedPrice);
                dataOut.writeInt(iData.highPrice); // Buy Rate
                dataOut.writeInt(iData.totalBidQty); //Buy Qty
                dataOut.writeInt(iData.lowPrice); //Sell Price
                dataOut.writeInt(iData.totalSellQty); // Sell Qty
                dataOut.writeInt(iData.tradedVolume);
                dataOut.writeInt(iData.lowerCircuit);
                dataOut.writeInt(iData.upperCircuit);
                dataOut.writeLong(iData.timeInMillis);
                if (isAsciiRequired){
                    String asciiHeader = "1^15MIN^*^4.1!" + iData.scripCode + "^";
                    String asciiData = "^1^4^1^" + iData.timeStamp + "^" + iData.scripCode + "^";
                    asciiData += "N^^" + format(iData.lastTradedPrice) + "^^^^^^";
                    asciiData += format(iData.highPrice) + "^" + format(iData.lowPrice) + "^^^";
                    asciiData += iData.totalBidQty + "^" + iData.totalSellQty + "^^^^^^^^^^";
                    String finalData = asciiHeader + asciiData.length() + asciiData;
                    streamAscii(finalData);
                }
                return byteArray.toByteArray();
            }
            
        } else {
            return null;
        }
        return null;
    }
    
    private String sendOpenInterest(IData iData) throws Exception {
        int exchangeCode = Types.NSE;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        NSEOutputStream dataOut = new NSEOutputStream(byteArray);
        dataOut.writeByte(1);
        dataOut.writeByte(exchangeCode);
        dataOut.writeByte(2);
        int length = String.valueOf(iData.scripCode).length();
        dataOut.writeByte(length);
        dataOut.writeBytes(String.valueOf(iData.scripCode));
        dataOut.writeInt(iData.openInterest);
        dataOut.writeInt(iData.lastTradedPrice);
        dataOut.writeInt(iData.tradedVolume);
        byte[] buffer = byteArray.toByteArray();
        String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
        return s;
    }
    
     private byte[] sendOpenInterest_TCP(IData iData) throws Exception {
        int exchangeCode = Types.NSE;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        NSEOutputStream dataOut = new NSEOutputStream(byteArray);
        dataOut.writeByte(1);
        dataOut.writeByte(exchangeCode);
        dataOut.writeByte(12);
        int length = String.valueOf(iData.scripCode).length();
        //length of this packet top to bottom
        // entire packet + 1 byte length scripcode + length of the scripcode 
        dataOut.writeByte(20 + 1 + length); 
        dataOut.writeByte(length);
        dataOut.writeBytes(String.valueOf(iData.scripCode));
        dataOut.writeInt(iData.openInterest);
        dataOut.writeInt(iData.lastTradedPrice);
        dataOut.writeInt(iData.tradedVolume);
        return byteArray.toByteArray();
        
    }
     
    private String sendMarketPictureBroadcast_WS(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (doBroadcast) {
            
            String LTT = " ";
            try{
                SimpleDateFormat sFddMMMYYYY = new SimpleDateFormat("dd-MMM-YYYY hh:mm:ss");
                long mSecond = Long.parseLong("1000"); 

                long lFinalTicks = 0L;
                lFinalTicks = iData.timeInMillis * 0x3155328L;

                long lLTT =     iData.timeInMillis* mSecond + Long.parseLong("315513000000");
                LTT = sFddMMMYYYY.format(lLTT);
                
            }catch(Exception ex){
                DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                Calendar calendar = Calendar.getInstance();
                long milliSeconds= calendar.getTimeInMillis();
                calendar.setTimeInMillis(milliSeconds);
                LTT = formatter.format(calendar.getTime());
            }

            int exchangeCode = Types.NSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            NSEOutputStream dataOut = new NSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(1);
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(length);
            
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            // Next 3 line is written by Nirmal
            dataOut.writeInt(iData.lastTradedPrice);
            if(iData.scripCode==1232){
                System.out.println(iData.lastTradedPrice);
            }
            // Next 2 to lines needs to be approved by HARI 
            dataOut.writeInt(iData.closePrice);
            //      dataOut.writeInt(iData.prevClosePrice);
            dataOut.writeInt(iData.tradedValue);
            
            dataOut.writeInt(iData.mDepth[0][0]);
            dataOut.writeInt(iData.mDepth[0][1]);
            dataOut.writeInt(iData.mDepth[0][2]);
            dataOut.writeInt(iData.mDepth[0][3]);
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
           // Next line is commented by Nirmal
          //  dataOut.writeInt(iData.closePrice);
          //  dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.lastTradedQty);
            dataOut.writeInt(iData.weightedAverage);
            dataOut.writeInt(Double.valueOf(iData.d_totalBidQty).intValue());
            dataOut.writeInt(Double.valueOf(iData.d_totalSellQty).intValue());
            // Next 2 lines is written by Nirmal. Instructed by HARI        
            dataOut.writeInt(iData.lowerCircuit);
            dataOut.writeInt(iData.upperCircuit);
            dataOut.writeInt(iData.tradedValue);
            // Next 3 line is written by Nirmal
            LTT=(LTT.trim());
            int lenTime = LTT.length();
            dataOut.writeByte(lenTime);
            dataOut.writeBytes(LTT);
            ScripData sD;
            // Modified by Nirmal
            sD = scripFeedTimeBased.get(iData.scripCode);
            if(sD==null){
               sD = new ScripData(); 
            }
           // ScripData sD = new ScripData();
            sD.scripCode = iData.scripCode;
            sD.previousLTP = sD.lastTradedPrice;
            sD.lastTradedPrice= iData.lastTradedPrice;
            sD.prevClosePrice = sD.lastTradedPrice;
            sD.alreadySend = false;
            scripFeedTimeBased.put(iData.scripCode, sD);
            // Modified by Nirmal
            
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;

        } else {
            return null;
        }
    }

    private String format(int v){
        return nF.format(v / 100.00);
    }
    
    private byte[] sendMarketPictureBroadcast_TCP(IData iData) throws Exception {
        boolean doBroadcast = true;
        if (dprChangeMap.containsKey(iData.scripCode)){
            IData iData2 = dprChangeMap.get(iData.scripCode);
            iData.lowerCircuit = iData2.lowerCircuit;
            iData.upperCircuit = iData2.upperCircuit;
        }
        if (doBroadcast) {
            
            String LTT = " ";
            switch (runningExchange) {
                case Types.iRunningExchange_NSE:
                {
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(1);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(1);
                    int length = String.valueOf(iData.scripCode).length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(76 + 1 + length); 
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeInt(iData.lastTradedPrice);
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.mDepth[0][0]); //BestBuyPrice
                    dataOut.writeInt(iData.mDepth[0][1]); //BestBuyQty
                    dataOut.writeInt(iData.mDepth[0][2]); //BestSellPrice
                    dataOut.writeInt(iData.mDepth[0][3]); //BestSellQty
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.lastTradedQty);
                    dataOut.writeInt(iData.weightedAverage);
                    dataOut.writeInt(Double.valueOf(iData.d_totalBidQty).intValue());
                    dataOut.writeInt(Double.valueOf(iData.d_totalSellQty).intValue());
                    dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeLong(iData.timeInMillis);
                    
                    if (isAsciiRequired){
                        String asciiHeader = "1^15MIN^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^1^4^1^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "N^^" + format(iData.lastTradedPrice) + "^" + format(iData.mDepth[0][0]) + "^";
                        asciiData += "^" + iData.mDepth[0][1] + "^" + format(iData.mDepth[0][2])  + "^" + iData.mDepth[0][3] + "^";
                        asciiData += format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.openPrice) + "^";
                        asciiData += format(iData.closePrice) + "^" + iData.totalBidQty  + "^" + iData.totalSellQty + "^";
                        asciiData += iData.lastTradedQty + "^" + iData.tradedVolume + "^" + iData.tradedValue + "^";
                        asciiData += "^^^^" + format(iData.weightedAverage) + "^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                case Types.iRunningExchange_FONSE:
                {
                    DerivativesScrip dScrip = derivativeMappingScrips.get(iData.scripCode);
                    if (dScrip == null){
                        return null;
                    }
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(111);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(5);
                    int length = String.valueOf(iData.scripCode).length();
                    int usLength = dScrip.cmToken.length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(66 + 1 + length + 1 + 12 + usLength); 
                    
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeByte(usLength);
                    dataOut.writeBytes(dScrip.cmToken);
                    dataOut.writeByte(dScrip.isFuture ? 1 : 0);
                    dataOut.writeInt(iData.lastTradedPrice);
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.mDepth[0][0]);
                    dataOut.writeInt(iData.mDepth[0][1]);
                    dataOut.writeInt(iData.mDepth[0][2]);
                    dataOut.writeInt(iData.mDepth[0][3]);
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.lastTradedQty);
                    dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeInt(iData.weightedAverage);
                    dataOut.writeInt(Double.valueOf(iData.d_totalBidQty).intValue());
                    dataOut.writeInt(Double.valueOf(iData.d_totalSellQty).intValue());
                    dataOut.writeLong(iData.timeInMillis);
                    dataOut.writeByte(dScrip.isIndex ? 1 : 0);
                    if (isAsciiRequired){
                        String asciiHeader = "1^LIVE^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^11^4^5^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "^^^^^" + format(iData.openPrice) + "^" + format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.closePrice) + "^" + format(iData.lastTradedPrice) + "^^^" + iData.tradedVolume + "^" + iData.tradedValue + "^^^" + format(iData.mDepth[0][0]) + "^";
                        asciiData += "^" + iData.mDepth[0][2] + "^" + format(iData.mDepth[0][1])  + "^" + iData.mDepth[0][3] + "^^^" + iData.lastTradedQty + "^^^" + format(iData.weightedAverage) + "^" +  iData.totalBidQty  + "^" + iData.totalSellQty + "^^^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                
                case Types.iRunningExchange_CURNSE:
                {
                    DerivativesScrip dScrip = derivativeMappingScrips.get(iData.scripCode);
                    if (dScrip == null){
                        return null;
                    }
                    int exchangeCode = runningExchange;
                    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                    NSEOutputStream dataOut = new NSEOutputStream(byteArray);
                    dataOut.writeByte(111);
                    dataOut.writeByte(exchangeCode);
                    dataOut.writeByte(5);
                    int length = String.valueOf(iData.scripCode).length();
                    int usLength = dScrip.cmToken.length();
                    //length of this packet top to bottom
                    // entire packet + 1 byte length scripcode + length of the scripcode 
                    dataOut.writeByte(66 + 1 + length + 1 + 12 + usLength); 
                    
                    dataOut.writeByte(length);
                    dataOut.writeBytes(String.valueOf(iData.scripCode));
                    dataOut.writeByte(usLength);
                    dataOut.writeBytes(dScrip.cmToken);
                    dataOut.writeByte(dScrip.isFuture ? 1 : 0);
                    dataOut.writeInt(iData.lastTradedPrice);
                    dataOut.writeInt(iData.closePrice);
                    dataOut.writeInt(iData.mDepth[0][0]);
                    dataOut.writeInt(iData.mDepth[0][1]);
                    dataOut.writeInt(iData.mDepth[0][2]);
                    dataOut.writeInt(iData.mDepth[0][3]);
                    dataOut.writeInt(iData.tradedVolume);
                    dataOut.writeInt(iData.highPrice);
                    dataOut.writeInt(iData.lowPrice);
                    dataOut.writeInt(iData.openPrice);
                    dataOut.writeInt(iData.lastTradedQty);
					dataOut.writeInt(iData.lowerCircuit);
                    dataOut.writeInt(iData.upperCircuit);
                    dataOut.writeInt(iData.weightedAverage);
                    dataOut.writeInt(Double.valueOf(iData.d_totalBidQty).intValue());
                    dataOut.writeInt(Double.valueOf(iData.d_totalSellQty).intValue());
                    dataOut.writeLong(iData.timeInMillis);
                    dataOut.writeByte(dScrip.isIndex ? 1 : 0);
                    if (isAsciiRequired){
                        String asciiHeader = "1^LIVE^*^4.1!" + iData.scripCode + "^";
                        String asciiData = "^11^4^5^" + iData.timeInMillis + "^" + iData.scripCode + "^";
                        asciiData += "^^^^^" + format(iData.openPrice) + "^" + format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.closePrice) + "^" + format(iData.lastTradedPrice) + "^^^" + iData.tradedVolume + "^" + iData.tradedValue + "^^^" + format(iData.mDepth[0][0]) + "^";
                        asciiData += "^" + iData.mDepth[0][2] + "^" + format(iData.mDepth[0][1])  + "^" + iData.mDepth[0][3] + "^^^" + iData.lastTradedQty + "^^^" + format(iData.weightedAverage) + "^" +  iData.totalBidQty  + "^" + iData.totalSellQty + "^^^^";
                        String finalData = asciiHeader + asciiData.length() + asciiData;
                        streamAscii(finalData);
                    }
                    return byteArray.toByteArray();
                }
                default:
                    return null;
            }
        } else {
            return null;
        }
    }
    
    public String sendMDepthDataOverBinary(IData iData) {
        try {
            int exchangeCode = Types.NSE;
            String LTT = " ";
            try{
            DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            long milliSeconds= iData.timeInMillis;
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(milliSeconds);
            LTT = formatter.format(calendar.getTime());
            }catch(Exception ex){}
            
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            NSEOutputStream dataOut = new NSEOutputStream(byteArray);
            dataOut.writeByte(2);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(1);
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            dataOut.writeInt(iData.mDepth[0][0]);
            dataOut.writeInt(iData.mDepth[0][1]);
            dataOut.writeInt(iData.mDepth[0][2]);
            dataOut.writeInt(iData.mDepth[0][3]);
            if (exchangeCode == 0) {
                dataOut.writeShort(iData.mDepth[0][4]);
                dataOut.writeShort(iData.mDepth[0][5]);
            }
            dataOut.writeInt(iData.mDepth[1][0]);
            dataOut.writeInt(iData.mDepth[1][1]);
            dataOut.writeInt(iData.mDepth[1][2]);
            dataOut.writeInt(iData.mDepth[1][3]);
            if (exchangeCode == 0) {
                dataOut.writeShort(iData.mDepth[1][4]);
                dataOut.writeShort(iData.mDepth[1][5]);
            }
            dataOut.writeInt(iData.mDepth[2][0]);
            dataOut.writeInt(iData.mDepth[2][1]);
            dataOut.writeInt(iData.mDepth[2][2]);
            dataOut.writeInt(iData.mDepth[2][3]);
            if (exchangeCode == 0) {
                dataOut.writeShort(iData.mDepth[2][4]);
                dataOut.writeShort(iData.mDepth[2][5]);
            }
            dataOut.writeInt(iData.mDepth[3][0]);
            dataOut.writeInt(iData.mDepth[3][1]);
            dataOut.writeInt(iData.mDepth[3][2]);
            dataOut.writeInt(iData.mDepth[3][3]);
            if (exchangeCode == 0) {
                dataOut.writeShort(iData.mDepth[3][4]);
                dataOut.writeShort(iData.mDepth[3][5]);
            }
            dataOut.writeInt(iData.mDepth[4][0]);
            dataOut.writeInt(iData.mDepth[4][1]);
            dataOut.writeInt(iData.mDepth[4][2]);
            dataOut.writeInt(iData.mDepth[4][3]);
            if (exchangeCode == 0) {
                dataOut.writeShort(iData.mDepth[4][4]);
                dataOut.writeShort(iData.mDepth[4][5]);
            }

            //dataOut.writeInt(Integer.parseInt(iData.tradedValue) * 100);
            dataOut.writeLong(LTT.length());
            dataOut.writeBytes(LTT);
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;
        } catch (IOException e) {
            return null;
        }
    }

    
    public byte[] sendMDepthDataOverBinary_TCP(IData iData) {
        try {
            int exchangeCode = runningExchange;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            NSEOutputStream dataOut = new NSEOutputStream(byteArray);
            dataOut.writeByte(2);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(4);
            int length = String.valueOf(iData.scripCode).length();
            //length of this packet top to bottom
            // entire packet + 1 byte length scripcode + length of the scripcode 
            dataOut.writeByte(113 + 1 + length); 
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            dataOut.writeByte(5);
            dataOut.writeInt(iData.mDepth[0][0]);
            dataOut.writeInt(iData.mDepth[0][1]);
            dataOut.writeInt(iData.mDepth[0][2]);
            dataOut.writeInt(iData.mDepth[0][3]);
            if (exchangeCode != 1) {
                dataOut.writeShort(iData.mDepth[0][4]);
                dataOut.writeShort(iData.mDepth[0][5]);
            }
            dataOut.writeInt(iData.mDepth[1][0]);
            dataOut.writeInt(iData.mDepth[1][1]);
            dataOut.writeInt(iData.mDepth[1][2]);
            dataOut.writeInt(iData.mDepth[1][3]);
            if (exchangeCode != 1) {
                dataOut.writeShort(iData.mDepth[1][4]);
                dataOut.writeShort(iData.mDepth[1][5]);
            }
            dataOut.writeInt(iData.mDepth[2][0]);
            dataOut.writeInt(iData.mDepth[2][1]);
            dataOut.writeInt(iData.mDepth[2][2]);
            dataOut.writeInt(iData.mDepth[2][3]);
            if (exchangeCode != 1) {
                dataOut.writeShort(iData.mDepth[2][4]);
                dataOut.writeShort(iData.mDepth[2][5]);
            }
            dataOut.writeInt(iData.mDepth[3][0]);
            dataOut.writeInt(iData.mDepth[3][1]);
            dataOut.writeInt(iData.mDepth[3][2]);
            dataOut.writeInt(iData.mDepth[3][3]);
            if (exchangeCode != 1) {
                dataOut.writeShort(iData.mDepth[3][4]);
                dataOut.writeShort(iData.mDepth[3][5]);
            }
            dataOut.writeInt(iData.mDepth[4][0]);
            dataOut.writeInt(iData.mDepth[4][1]);
            dataOut.writeInt(iData.mDepth[4][2]);
            dataOut.writeInt(iData.mDepth[4][3]);
            if (exchangeCode != 1) {
                dataOut.writeShort(iData.mDepth[4][4]);
                dataOut.writeShort(iData.mDepth[4][5]);
            }

            //dataOut.writeInt(Integer.parseInt(iData.tradedValue) * 100);
            dataOut.writeLong(iData.timeInMillis);
            if (isAsciiRequired){
                String asciiHeader = "1^LIVE^*^4.2!" + iData.scripCode + "^";
                String asciiData = "^10^4^1^" + iData.timeInMillis + "^" + iData.scripCode + "^5^";
                for(int i = 0; i < 5; i++){
                    asciiData += format(iData.mDepth[i][0]) + "^" + iData.mDepth[i][1] + "^" + iData.mDepth[i][4] + "^0^";
                    asciiData += format(iData.mDepth[i][2]) + "^" + iData.mDepth[i][3] + "^" + iData.mDepth[i][5] + "^0^";
                }
                String finalData = asciiHeader + asciiData.length() + asciiData;
                streamAscii(finalData);
            }
            return byteArray.toByteArray();
            
        } catch (IOException e) {
            return null;
        }
    }
    
    private String sendIndexBroadcast_WS(IData iData) throws Exception {
        int exchangeCode = Types.NSE;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        NSEOutputStream dataOut = new NSEOutputStream(byteArray);

        try {
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(3);
            int length = iData.scripId.length();
            dataOut.writeByte(length);
            dataOut.writeBytes(iData.scripId.trim());
            int ltp = iData.lastTradedPrice;
            int cp = iData.closePrice;
            int absChange = ltp - cp;
            dataOut.writeInt(ltp);
            dataOut.writeInt(absChange);
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] sendIndexBroadcast_TCP(IData iData) throws Exception {
        int exchangeCode = runningExchange;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        NSEOutputStream dataOut = new NSEOutputStream(byteArray);

        try {
            //byte[] tS = iData.timeStamp.getBytes();
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(3);
            
            int length = iData.scripId.length();
            //length of this packet top to bottom
            // entire packet + 1 byte length scripcode + length of the scripcode  + 1 byte length timestamp + length of the timestamp

            //dataOut.writeByte(26 + length + tS.length); 
            //mLog.info(iData.scripId.toUpperCase());
            dataOut.writeByte(34 + length); 
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripId).toUpperCase());
            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.closePrice);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
            //dataOut.writeByte(tS.length);
            dataOut.writeLong(iData.timeInMillis);
			if (isAsciiRequired){
                String asciiHeader = "1^15MIN^*^4.1!" + iData.scripCode + "^";
                String asciiData = "^1^4^1^" + iData.timeStamp + "^" + iData.scripCode + "^";
                asciiData += "N^^" + format(iData.lastTradedPrice) + "^^^^^^";
                asciiData += format(iData.highPrice) + "^" + format(iData.lowPrice) + "^" + format(iData.openPrice) + "^";
                asciiData += format(iData.closePrice) + "^^^^^^^^^^^^";
                String finalData = asciiHeader + asciiData.length() + asciiData;
                streamAscii(finalData);
            }
            return byteArray.toByteArray();
            
        } catch (IOException e) {
            return null;
        }
    }
    
    private void streamRawbuffer(byte[] buffer) {
        try {
            DatagramPacket dPacket = new DatagramPacket(buffer, buffer.length);
            dPacket.setAddress(sAddress);
            dPacket.setPort(sPort);
            mSocket.setTimeToLive(BroadcastInfo.timeToLive);
            mSocket.send(dPacket);
            if (this.isAdditionalOutputEnabled){
                for(AdditionalIps aIps : additionalIps){
                    dPacket.setAddress(aIps.additionalIp);
                    dPacket.setPort(aIps.additionalPort);
                    mSocket.send(dPacket);
                }
            }
        } catch (IOException lEx) {
            mLog.error("Unable to Stream data, Detailed Msg is : " + lEx.getMessage());
        }
    }

    private void stream(String aData) {
        try {
            byte[] buffer = aData.getBytes();
            DatagramPacket dPacket = new DatagramPacket(buffer, buffer.length);
            dPacket.setAddress(sAddress);
            dPacket.setPort(sPort);
            mSocket.setTimeToLive(BroadcastInfo.timeToLive);
            mSocket.send(dPacket);
            if (this.isAdditionalOutputEnabled){
                for(AdditionalIps aIps : additionalIps){
                    dPacket.setAddress(aIps.additionalIp);
                    dPacket.setPort(aIps.additionalPort);
                    mSocket.send(dPacket);
                }
            }
        } catch (IOException lEx) {
            mLog.error("Unable to Stream data, Detailed Msg is : " + lEx.getMessage());
        }
    }

    private void streamAscii(String aData) {
        try {
            byte[] buffer = aData.getBytes();
            DatagramPacket dPacket = new DatagramPacket(buffer, buffer.length);
            dPacket.setAddress(sAsciiAddress);
            dPacket.setPort(sAsciiPort);
            mSocket1.setTimeToLive(BroadcastInfo.timeToLive);
            mSocket1.send(dPacket);
        } catch (IOException lEx) {
            mLog.error("Unable to Stream ascii data, Detailed Msg is : " + lEx.getMessage());
        }
    }

    public static class BroadcastInfo {
        public static final int timeToLive = 5;
    }

    class AdditionalIps{
        public InetAddress additionalIp;
        public int additionalPort;
    }
   
   
}
