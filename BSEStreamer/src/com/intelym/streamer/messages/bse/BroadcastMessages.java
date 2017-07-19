/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.bse;

import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.data.IData;
import com.intelym.streamer.headers.CommonMessage;
import java.text.SimpleDateFormat;

/**
 *
 * @author Hari Nair
 */
public class BroadcastMessages extends CommonMessage{
    
    protected SimpleDateFormat sF = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    
    public boolean isLevel2Available(){
        return false;
    }
    
    public IData processLevel1Messages(BSEInputStream in) throws Exception {
        return null;
    }
    
    public IData processLevel2Messages(BSEInputStream in) throws Exception {
        return null;
    }
}
