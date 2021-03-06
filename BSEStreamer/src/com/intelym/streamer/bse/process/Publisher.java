/*
 * Publishes the broadcast to Riwa on IGMP or Unicast Broadcast
 */
package com.intelym.streamer.bse.process;

import com.intelym.logger.LoggerFactory;
import com.intelym.logger.QuickLogger;
import com.intelym.streamer.common.IndicesInfo;
import com.intelym.streamer.communication.BSEOutputStream;
import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.streamer.data.IData;
import com.intelym.streamer.data.ScripData;
import com.intelym.streamer.headers.Constants;
import com.intelym.streamer.headers.Types;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.text.NumberFormat;
import java.util.ArrayList;
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

    private static final QuickLogger mLog = LoggerFactory.getLogger(Publisher.class);
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
    private NumberFormat nF;
    private HashMap<String, IndicesInfo> indexMap;
    private final TreeMap<Integer, ScripData> scripsMap;
    private IndexLoader indexLoader = null;
    private boolean isAsciiRequired = false;
    
    private boolean isAdditionalOutputEnabled = false;
    private List<AdditionalIps> additionalIps = null;

    public Publisher(StreamerConfiguration aConfiguration, TreeMap sMap) {
        scripsMap = sMap;
        sConfiguration = aConfiguration;
        publisherQueue = new LinkedBlockingQueue();
        indexLoader = new IndexLoader();
        nF = NumberFormat.getInstance();
        nF.setMinimumFractionDigits(2);
        nF.setMaximumFractionDigits(2);
        nF.setGroupingUsed(false);
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
        try{
            indexMap = indexLoader.getIndexMap();
        }catch(Exception e){
            mLog.info("Unable to connect to database to load derivative scrips");
                    
        }

    }

    private IndicesInfo getIndex(int id) {
        return new IndicesInfo(id);
    }

    /**
     * this method is not used
     */
    private void loadIndex() {
        try {

            indexMap.put("SENSEX", getIndex(16));
            indexMap.put("MIDCAP", getIndex(59));
            indexMap.put("SMLCAP", getIndex(60));
            indexMap.put("BSE100", getIndex(22));
            indexMap.put("BSE200", getIndex(23));
            indexMap.put("BSE500", getIndex(17));
            indexMap.put("GREENX", getIndex(75));
            indexMap.put("CARBON", getIndex(77));
            indexMap.put("BSEIPO", getIndex(72));
            indexMap.put("SMEIPO", getIndex(76));
            indexMap.put("DOL30", getIndex(47));
            indexMap.put("DOL100", getIndex(65));
            indexMap.put("DOL200", getIndex(48));
            indexMap.put("AUTO", getIndex(42));
            indexMap.put("BANKEX", getIndex(53));
            indexMap.put("BSE CG", getIndex(25));
            indexMap.put("BSE CD", getIndex(27));
            indexMap.put("BSEFMC", getIndex(29));
            indexMap.put("BSE HC", getIndex(31));
            indexMap.put("BSE IT", getIndex(33));
            indexMap.put("METAL", getIndex(35));
            indexMap.put("OILGAS", getIndex(37));
            indexMap.put("POWER", getIndex(69));
            indexMap.put("BSEPSU", getIndex(44));
            indexMap.put("REALTY", getIndex(67));
            indexMap.put("TECK", getIndex(45));
            indexMap.put("INFRA", getIndex(79));

        } catch (Exception e) {
        }
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
                delayTimeDifference = Long.valueOf(timeDifference);
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

//            if (port1 != null & !port1.equals("")) {
//                sPort1 = Integer.parseInt(port1);
//            }
//
//            if (sPort1 > 0) {
//                mSocket1 = new MulticastSocket();
//            }
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
        }catch (Exception e) {
            mLog.error("Other Exception : " + e.getMessage());
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
                try {
                    IData iData = publisherQueue.poll(100l, TimeUnit.NANOSECONDS);
                    if (iData != null) {
                        publishData(iData);
                    }
                    // publisherQueue.wait();
                } catch (InterruptedException e) {
                }
            }
        } catch (Exception e) {
            isRunning = false;
        }
    }

    public void publishData(IData iData) {
        String data;
        String mDepthData;
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
                        case Types.BC_MARKETWATCH_BSEM:
                            data = sendMarketPictureBroadcast_WS(iData);
                            mDepthData = sendMDepthDataOverBinary(iData);
                            if (data != null) {
                                stream(data);
                            }
                            if (mDepthData != null) {
                                stream(data);
                            }
                            break;
                        case Types.BC_MARKETWATCH_BSET:
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
                        case Types.BC_MARKETDEPTH_NOTIME_DEPTH:
                        case Types.BC_MARKETWATCH_BSEM:
                            buffer = sendMarketPictureBroadcast_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            mDepthBuffer = sendMDepthDataOverBinary_TCP(iData);
                            if (mDepthBuffer != null) {
                                streamRawbuffer(mDepthBuffer);
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

                        case Types.BC_MARKETWATCH_BSET:
                            buffer = sendOpenInterest_TCP(iData);
                            if (buffer != null) {
                                streamRawbuffer(buffer);
                            }
                            break;
                        case Types.BC_MARKETDEPTH:
                            break;
                    }
                    break;
            }

        } catch (Exception lEx) {
            mLog.error("Publish to Quick Fails, Detailed Msg is : " + lEx.getMessage());
        }
    }

    
    private byte[] sendMarketPictureBroadcast_TCP(IData iData) throws Exception {
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

            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(1);
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(76 + 1 + length);
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.closePrice == 0 ? iData.prevClosePrice : iData.closePrice);
            dataOut.writeInt(iData.mDepth[0][0]); //BestBuyPrice
            dataOut.writeInt(iData.mDepth[0][1]); //BestBuyQty
            dataOut.writeInt(iData.mDepth[0][3]); //BestSellPrice
            dataOut.writeInt(iData.mDepth[0][4]); //BestSellQty :: BestOfferQty
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
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
        } else {
            return null;
        }
    }
    
    public byte[] sendMDepthDataOverBinary_TCP(IData iData) {
        try {
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
            dataOut.writeByte(2);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(4);
            int length = String.valueOf(iData.scripCode).length();
           
            dataOut.writeByte(93 + 1 + 40 + length);
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            dataOut.writeByte(5); // no of records
            dataOut.writeInt(iData.mDepth[0][0]);
            dataOut.writeInt(iData.mDepth[0][1]);
            dataOut.writeInt(iData.mDepth[0][3]);
            dataOut.writeInt(iData.mDepth[0][4]);
            dataOut.writeInt(iData.mDepth[0][2]);
            dataOut.writeInt(iData.mDepth[0][5]);
            
            dataOut.writeInt(iData.mDepth[1][0]);
            dataOut.writeInt(iData.mDepth[1][1]);
            dataOut.writeInt(iData.mDepth[1][3]);
            dataOut.writeInt(iData.mDepth[1][4]);
            dataOut.writeInt(iData.mDepth[1][2]);
            dataOut.writeInt(iData.mDepth[1][5]);
            
            dataOut.writeInt(iData.mDepth[2][0]);
            dataOut.writeInt(iData.mDepth[2][1]);
            dataOut.writeInt(iData.mDepth[2][3]);
            dataOut.writeInt(iData.mDepth[2][4]);
            dataOut.writeInt(iData.mDepth[2][2]);
            dataOut.writeInt(iData.mDepth[2][5]);
            
            dataOut.writeInt(iData.mDepth[3][0]);
            dataOut.writeInt(iData.mDepth[3][1]);
            dataOut.writeInt(iData.mDepth[3][3]);
            dataOut.writeInt(iData.mDepth[3][4]);
            dataOut.writeInt(iData.mDepth[3][2]);
            dataOut.writeInt(iData.mDepth[3][5]);
            
            dataOut.writeInt(iData.mDepth[4][0]);
            dataOut.writeInt(iData.mDepth[4][1]);
            dataOut.writeInt(iData.mDepth[4][3]);
            dataOut.writeInt(iData.mDepth[4][4]);
            dataOut.writeInt(iData.mDepth[4][2]);
            dataOut.writeInt(iData.mDepth[4][5]);
            
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
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
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
            dataOut.writeByte(iData.timeStamp.length());
            dataOut.writeBytes(iData.timeStamp);
            dataOut.writeByte(Integer.valueOf(iData.tradingSession).byteValue());
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

    private byte[] sendMarketPictureBroadcast1906_TCP(IData iData) throws Exception {
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
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(2);
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(36 + 1 + length);
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));

            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.totalBidQty);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.totalSellQty);
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeLong(iData.timeInMillis);
            return byteArray.toByteArray();
            
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
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
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
            dataOut.writeByte(iData.timeStamp.length());
            dataOut.writeBytes(iData.timeStamp);
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
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(13);
            
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(52 + 1 + length);
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.tradedValue);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
            dataOut.writeInt(iData.closePrice);
            dataOut.writeInt(iData.lastTradedQty);
            dataOut.writeInt(iData.weightedAverage);
            dataOut.writeInt(iData.totalBidQty);
            dataOut.writeInt(iData.totalSellQty);
            dataOut.writeLong(iData.timeInMillis);
            return byteArray.toByteArray();
            
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
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
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
            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(2);
            
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(36 + 1 + length);
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));

            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.totalBidQty);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.totalSellQty);
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeLong(iData.timeInMillis);
            return byteArray.toByteArray();
            
        } else {
            return null;
        }
    }
    
    private String sendMarketPictureBroadcast_WS(IData iData) throws Exception {
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

            int exchangeCode = Types.BSE;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(1);
            int length = String.valueOf(iData.scripCode).length();
            dataOut.writeByte(length);
            dataOut.writeBytes(String.valueOf(iData.scripCode));
            dataOut.writeInt(iData.mDepth[0][0]);
            dataOut.writeInt(iData.mDepth[0][1]);
            dataOut.writeInt(iData.mDepth[0][2]);
            dataOut.writeInt(iData.mDepth[0][3]);
            dataOut.writeInt(iData.tradedVolume);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
            dataOut.writeInt(iData.closePrice);
            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.lastTradedQty);
            dataOut.writeInt(iData.weightedAverage);
            dataOut.writeInt(iData.totalBidQty);
            dataOut.writeInt(iData.totalSellQty);
            dataOut.writeInt(iData.tradedValue);
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;

        } else {
            return null;
        }
    }

    public String sendMDepthDataOverBinary(IData iData) {
        try {
            int exchangeCode = Types.BSE;

            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            BSEOutputStream dataOut = new BSEOutputStream(byteArray);
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
            dataOut.writeByte(iData.timeStamp.length());
            dataOut.writeBytes(iData.timeStamp);
            byte[] buffer = byteArray.toByteArray();
            String s = javax.xml.bind.DatatypeConverter.printBase64Binary(buffer);
            return s;
        } catch (Exception e) {
            return null;
        }
    }

    private byte[] sendIndexBroadcast_TCP(IData iData) throws Exception {
        int exchangeCode = Types.BSE;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        BSEOutputStream dataOut = new BSEOutputStream(byteArray);

        try {
            //byte[] tS = iData.timeStamp.getBytes();
            byte[] sC = iData.scripId.trim().getBytes();
            dataOut.writeByte(1);
            dataOut.writeByte(exchangeCode);
            dataOut.writeByte(3);
            //dataOut.writeByte(24 + 1 + tS.length + 1 + sC.length);
            dataOut.writeByte(24 + 1 + 8 + 1 + sC.length);
            dataOut.writeByte(sC.length);
            dataOut.write(sC);
            dataOut.writeInt(iData.lastTradedPrice);
            dataOut.writeInt(iData.closePrice);
            dataOut.writeInt(iData.highPrice);
            dataOut.writeInt(iData.lowPrice);
            dataOut.writeInt(iData.openPrice);
            //dataOut.writeByte(tS.length);
            //dataOut.write(tS);
            dataOut.writeLong(iData.timeInMillis);
            return byteArray.toByteArray();
            
        } catch (IOException e) {
            return null;
        }
    }
    
    private String sendIndexBroadcast_WS(IData iData) throws Exception {
        IndicesInfo iInfo = getIndexIds(iData.scripId.trim());
        if (iInfo == null) {
            return null;
        }
        iData.scripId = iInfo.indexId + "";
        int exchangeCode = Types.BSE;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        BSEOutputStream dataOut = new BSEOutputStream(byteArray);

        try {
            if (iData.tradingSession == 3 || iData.tradingSession == 1 || iData.tradingSession == 2) {
                iInfo.closePrice = iData.closePrice;
            } else {
                if (iInfo.closePrice != 0) {
                    iData.closePrice = iInfo.closePrice;
                }
            }

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
        } catch (Exception e) {
            return null;
        }
    }
    
    private byte[] sendOpenInterest_TCP(IData iData) throws Exception {
        int exchangeCode = Types.BSE;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        BSEOutputStream dataOut = new BSEOutputStream(byteArray);
        dataOut.writeByte(1);
        dataOut.writeByte(exchangeCode);
        dataOut.writeByte(12);
        int length = String.valueOf(iData.scripCode).length();
        //length of this packet top to bottom
        // entire packet + 1 byte length scripcode + length of the scripcode 
        dataOut.writeByte(12 + 1 + length); 
        dataOut.writeByte(length);
        dataOut.writeBytes(String.valueOf(iData.scripCode));
        dataOut.writeInt(iData.oiValue);
        dataOut.writeInt(iData.oiChange);
        dataOut.writeInt(iData.oiQty);
        return byteArray.toByteArray();
        
    }

    private IndicesInfo getIndexIds(String index) {
        IndicesInfo indexId = indexMap.get(index);
        return indexId;
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
    
    private void stream1(String aData) {
        try {
            if (mSocket1 == null && sPort1 <= 0) {
                stream(aData);
                return;
            }
            byte[] buffer = aData.getBytes();
            DatagramPacket dPacket = new DatagramPacket(buffer, buffer.length);
            dPacket.setAddress(sAddress);
            dPacket.setPort(sPort1);
            mSocket1.setTimeToLive(BroadcastInfo.timeToLive);
            mSocket1.send(dPacket);
        } catch (Exception lEx) {
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
