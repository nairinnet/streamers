package com.intelym.streamer.common;

import com.intelym.streamer.communication.NSEInputStream;
import java.text.SimpleDateFormat;

/**
 *
 * @author Hari Nair
 */
public class Header extends CommonMessage{

    public int slotNumber = 0;
    public int messageLength = -1;
    public int messageCode = -1;
    public int logTime = -1;
    public String alphaChar;
    public int errorCode = -1;
    public int  bcSeqNo = 01;
    public String timeStamp;
    
    
    public Header(){
        super();
    }
    
    public void process(NSEInputStream in) throws Exception{
        
    }
}
