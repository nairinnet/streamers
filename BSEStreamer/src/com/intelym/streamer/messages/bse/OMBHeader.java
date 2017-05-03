/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.messages.bse;

import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.headers.Header;

/**
 *
 * @author Hari Nair
 */
public class OMBHeader extends Header {
    
    
    /**
     * Processes the header as required in Open Message Bus
     * @param in
     * @throws Exception
     */
    @Override
    public void process(BSEInputStream in) throws Exception{
        slotNumber = readInt(in);
        messageLength = readInt(in);
        messageCode = readInt(in);
    }
}
