/*
 * Heart of the Streamer application. controlls the flow of the application
 */
package com.intelym.streamer.nse.process;

import com.intelym.database.ConnectionManager;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;
import com.intelym.streamer.common.Header;
import com.intelym.streamer.common.IndicesInfo;
import com.intelym.streamer.common.Types;
import com.intelym.streamer.communication.NSEInputStream;
import com.intelym.streamer.communication.nse.BroadcastManager;
import com.intelym.streamer.communication.nse.NNFHeader;
import com.intelym.streamer.config.StreamerConfiguration;
import com.intelym.streamer.data.IData;
import com.intelym.streamer.data.ScripData;
import com.intelym.streamer.messages.fonse.BroadcastTicker;
import com.intelym.streamer.messages.nse.BroadcastIndices;
import com.intelym.streamer.messages.nse.BroadcastMBOMBP;
import com.intelym.streamer.messages.nse.BroadcastOnlyMBP;
import com.intelym.streamer.messages.nse.CallAuctionMBP;
import java.util.ArrayList;
import java.util.TreeMap;
import javolution.util.FastMap;

/**
 *
 * @author Hari Nair
 */
public class ProcessController implements ProcessManager{
    
    private static RiwaLogger mLog = LoggerFactory.getLogger(ProcessController.class);
    private StreamerConfiguration eConfiguration = null;
    private BroadcastManager bManager = null;
    private Publisher aPublisher = null;
    private TreeMap<Integer, Long> scripsMap;
    private TreeMap<String, ArrayList> scripMapTimeBased; // Modified by Nirmal
    private FastMap<Integer, ScripData> allScripsMap;
    private int differenceTime = 1000;
    private int lastAvailableSession = -1;
    private TreeMap<String, IndicesInfo> indexMap = null;
    private int runningExchange = -1;
    public ProcessController(String config) {
        try {
            mLog.info("Reading streamer configuration data....");
            eConfiguration = StreamerConfiguration.newInstance(config);
            mLog.info("Streamer configuration reading is completed....");

            // Modified by Nirmal
            ConnectionManager cManager = new ConnectionManager(eConfiguration);
            cManager.connect();
            scripMapTimeBased= cManager.getIndexScripList();
            // Modified by Nirmal
            
            
            String tmp = eConfiguration.getString("RIWA.Streamer.DiffTimeout");
            if (tmp != null) {
                differenceTime = Integer.parseInt(tmp);
            }


            allScripsMap = new FastMap<>();
            
            aPublisher = new Publisher(eConfiguration, scripsMap, null,scripMapTimeBased );
            
            tmp = eConfiguration.getString("RIWA.Exchange");
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
        } catch (Exception lEx) {
            mLog.error("Processor Failed, Detailed Msg is : " + lEx.getMessage());
        }
    }

    public void startProcess(){
        try{
            mLog.info("Streamer Process is starting...........");
            String port = eConfiguration.getString("TAP.ListenerPort");
            String address = eConfiguration.getString("TAP.CircuitAddress");
            int sPort = new Integer(port).intValue();
            initiateBroadcastConnection(sPort, address);
            aPublisher.openConnection();
        }catch(Exception e){
            
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
    public Header processHeader(NSEInputStream in) throws Exception {
        try{
            NNFHeader header = new NNFHeader();
            Header hdr = header.process(in);
            return hdr;
        }catch(Exception e){}
        return null;
    }

    @Override
    public void processIncoming(NSEInputStream in, Header header) throws Exception {
        try{
            switch(runningExchange){
                case Types.iRunningExchange_NSE:
                    switch(header.messageCode){
                        case Types.NSE_MBO_MBP:
                            processMBOMBP(in, header);
                            break;
                        case Types.NSE_BROADCAST_CALL_AUCTION:
                            processCallAuctionMBP(in, header);
                            break;
                        case Types.NSE_CALL_AUCTION_MBP:
                            break;
                        case Types.NSE_MKT_WATCH:
                            break;
                        case Types.NSE_TICKER:
                            break;
                        case Types.NSE_ONLY_MBP:
                            processOnlyMBP(in, header);
                            break;
                        case Types.NSE_BROADCAST_INDICES:
                        case Types.NSE_BROADCAST_INDICES_VIX:
                            processBroadcastIndices(in, header);
                            break;
                    }
                    break;
                case Types.iRunningExchange_FONSE:
                    switch(header.messageCode){
                        case Types.NSE_MBO_MBP:
                            processMBOMBP(in, header);
                            break;
                        case Types.NSE_BROADCAST_CALL_AUCTION:
                            processCallAuctionMBP(in, header);
                            break;
                        case Types.NSE_CALL_AUCTION_MBP:
                            break;
                        case Types.NSE_MKT_WATCH:
                            break;
                        case Types.NSE_TICKER:
                            processTicker(in, header);
                            break;
                        case Types.NSE_ONLY_MBP:
                            processOnlyMBP_FONSE(in, header);
                            break;
                        case Types.NSE_BROADCAST_INDICES:
                            processBroadcastIndices(in, header);
                            break;
                    }
                    break;
                case Types.iRunningExchange_CURNSE:
                    switch(header.messageCode){
                        case Types.NSE_MBO_MBP:
                            processMBOMBP(in, header);
                            break;
                        case Types.NSE_BROADCAST_CALL_AUCTION:
                            processCallAuctionMBP(in, header);
                            break;
                        case Types.NSE_CALL_AUCTION_MBP:
                            break;
                        case Types.NSE_MKT_WATCH:
                            break;
                        case Types.NSE_TICKER:
                            processTicker(in, header);
                            break;
                        case Types.NSE_ONLY_MBP:
                          //  processOnlyMBP(in, header);
                            processOnlyMBP_FONSE(in, header);
                            break;
                    }
                    break;
                    
            }
        }catch(Exception e){
            
        }
    }

    private void streamToRiwa(IData iData) {
        aPublisher.add(iData);
    }
    
    public void processBroadcastIndices(NSEInputStream in, Header header){
        try{
            BroadcastIndices bMBO = new BroadcastIndices();
            IData iData = bMBO.processLevel1Messages(in);
            for(int i = 0; i < iData.noOfRecords; i++){
                IData iData2 = bMBO.processLevel2Messages(in);
                iData2.publishCode = Types.BC_INDEX;
                streamToRiwa(iData2);
            }
        }catch(Exception e){
            
        }
    }
    public void processCallAuctionMBP(NSEInputStream in, Header header){
        try{
            CallAuctionMBP bMBO = new CallAuctionMBP();
            IData iData = bMBO.processLevel1Messages(in);
            for(int i = 0; i < iData.noOfRecords; i++){
                IData iData2 = bMBO.processLevel2Messages(in);
                iData2.publishCode = Types.BC_MARKETWATCH_NSEM;
                streamToRiwa(iData2);
            }
        }catch(Exception e){
            
        }
    }
    
    public void processTicker(NSEInputStream in, Header header){
        try{
            BroadcastTicker ticker = new BroadcastTicker();
            IData iData = ticker.processLevel1Messages(in, header);
            for(int i = 0; i < iData.noOfRecords; i++){
                IData iData2 = ticker.processLevel2Messages(in, header);
                iData2.publishCode = Types.BC_MARKETWATCH_NSET;
                streamToRiwa(iData2);
            }
        }catch(Exception e){
            
        }
    }
    
    public void processOnlyMBP(NSEInputStream in, Header header){
        try{
            BroadcastOnlyMBP bMBO = new BroadcastOnlyMBP();
            IData iData = bMBO.processLevel1Messages(in);
            for(int i = 0; i < iData.noOfRecords; i++){
                IData iData2 = bMBO.processLevel2Messages(in);
                iData2.publishCode = Types.BC_MARKETWATCH_NSEM;
                streamToRiwa(iData2);
            }
        }catch(Exception e){
        }
    }
    
    public void processOnlyMBP_FONSE(NSEInputStream in, Header header){
        try{
            com.intelym.streamer.messages.fonse.BroadcastOnlyMBP bMBO = new com.intelym.streamer.messages.fonse.BroadcastOnlyMBP();
            IData iData = bMBO.processLevel1Messages(in);
            for(int i = 0; i < iData.noOfRecords; i++){
                IData iData2 = bMBO.processLevel2Messages(in);
                iData2.publishCode = Types.BC_MARKETWATCH_NSEM;
                streamToRiwa(iData2);
            }
        }catch(Exception e){
        }
    }
    
    public void processMBOMBP(NSEInputStream in, Header header){
        try{
            BroadcastMBOMBP bMBO = new BroadcastMBOMBP();
            IData iData = bMBO.processLevel2Messages(in);
            iData.publishCode = Types.BC_MARKETWATCH_NSEM;
            streamToRiwa(iData);
        }catch(Exception e){
            
        }
    }
    
     public void processMBOMBP_FONSE(NSEInputStream in, Header header){
        try{
            com.intelym.streamer.messages.fonse.BroadcastMBOMBP bMBO = new com.intelym.streamer.messages.fonse.BroadcastMBOMBP();
            IData iData = bMBO.processLevel2Messages(in);
            iData.publishCode = Types.BC_MARKETWATCH_NSEM;
            streamToRiwa(iData);
        }catch(Exception e){
            
        }
    }
     
    @Override
    public void connectionAborted() {
        try{
            
        }catch(Exception e){}
    }
    
    
}
