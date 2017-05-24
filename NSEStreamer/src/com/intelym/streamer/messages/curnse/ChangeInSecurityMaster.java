/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.curnse;

import com.intelym.streamer.common.CommonMessage;
import com.intelym.streamer.communication.NSEInputStream;
import com.intelym.streamer.data.IData;
import java.io.IOException;

/**
 *
 * @author Hari Nair
 */
public class ChangeInSecurityMaster extends CommonMessage{
    
    public ChangeInSecurityMaster() {
        
    }
    
    public void processLevel1Messages(NSEInputStream in){
        
    }
    
    public IData processLevel2Messages_DERIVATIVE(NSEInputStream in){
        IData iData = new IData();
        try{
            iData.scripCode = in.readInt();  //token
            //in.skipBytes(144); // skip until DPR
            in.skipBytes(135); // skip until DPR
            iData.lowerCircuit = in.readInt();
            iData.upperCircuit = in.readInt();
            in.skipBytes(in.available());
            
        }catch(IOException e){
            
        }
        return iData;
    }
    
    public IData processLevel2Messages(NSEInputStream in) throws Exception{
        IData iData = new IData();
        try{
            iData.scripCode = in.readShort();  //token
            //in.skipBytes(144); // skip until DPR
            in.skipBytes(28); // skip until DPR
            String creditRating = toChars(in, 17);
            String[] circuit=creditRating.split("-");//splits the string based on whitespace 
            iData.lowerCircuit = (int)(Double.parseDouble(circuit[0].trim())*100);
            iData.upperCircuit = (int)(Double.parseDouble(circuit[1].trim())*100);
            in.skipBytes(in.available());
            
        }catch(IOException e){
            
        }
        return iData;
    }
}
