
/*
 * Start up Class for NSE Streamer
 */
package com.intelym.streamer.nse.startup;

import com.intelym.streamer.nse.process.ProcessController;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;

/**
 *
 * @author Hari Nair
 */
public class BootStrap {

    private static RiwaLogger mLog = LoggerFactory.getLogger(BootStrap.class);
    
    public BootStrap(String config){
        Thread.currentThread().setName("NSE-Publisher-API");
        printVersionInfo();
        startProcessor(config);
    }
    
    private void startProcessor(String config){
        try{
            if(config != null){
                ProcessController pController = new ProcessController(config);
                pController.startProcess();
            }
        }catch(Exception e){
            mLog.error("Init Processor Failed, Detailed Msg is : " + e.getMessage());
        }
    }
    
    /**
     * Prints the version info, please update the method on every release.
     */
    private void printVersionInfo(){
        mLog.info("************ RIWA NSE Streamer ********************");
        mLog.info("************ Version 1.0 Build 1.00.000 *******");
        //mLog.info("Licensed to Bombay Stock Exchange for BSE Mobile Version");
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        if(args.length != 0){
            BootStrap gateway = new BootStrap(args[0]);
        }
        else {
            BootStrap gateway = new BootStrap("src/conf/nsestreamer.properties");
        }
    }
}
