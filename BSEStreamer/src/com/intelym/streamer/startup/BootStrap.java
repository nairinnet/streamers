
/*
 * Start up Class for RIWA Streamer
 */
package com.intelym.streamer.startup;

import com.intelym.logger.LoggerFactory;
import com.intelym.logger.QuickLogger;
import com.intelym.streamer.bse.process.ProcessController;

/**
 *
 * @author Hari Nair
 */
public class BootStrap {

    private static final QuickLogger mLog = LoggerFactory.getLogger(BootStrap.class);
    
    public BootStrap(String config){
        Thread.currentThread().setName("Quick-Publisher-API");
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
        mLog.info("************ Quick BSE Streamer ********************");
        mLog.info("************ Version 3.0 Build 1.00.100 *******");
        
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
            BootStrap gateway = new BootStrap("src/conf/bsestreamer.properties");
        }
    }
}
