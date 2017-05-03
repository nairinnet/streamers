/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.communication.nse;

import com.intelym.streamer.common.CommonMessage;
import com.intelym.streamer.common.Header;
import com.intelym.streamer.communication.NSEInputStream;

/**
 *
 * @author HariNair
 */
public class NNFHeader extends CommonMessage{
    
    public NNFHeader(){
        
    }
    
    public Header process(NSEInputStream in){
        Header header = new Header();
        try{
            skipBytes(in, 4);
            header.logTime = in.readInt();
            header.alphaChar = toChars(in, 2);
            header.messageCode = in.readShort();
            header.errorCode = in.readShort();
            header.bcSeqNo = in.readInt();
            skipBytes(in, 4);
            header.timeStamp = toChars(in, 8);
            skipBytes(in, 8);
            header.messageLength = in.readShort();
        }catch(Exception e){}
        return header;
    }
}
