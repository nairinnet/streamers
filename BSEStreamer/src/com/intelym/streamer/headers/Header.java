package com.intelym.streamer.headers;

import com.intelym.streamer.communication.BSEInputStream;
import java.text.SimpleDateFormat;

/**
 *
 * @author Hari Nair
 */
public class Header extends CommonMessage{

    public int slotNumber = 0;
    public int messageLength = -1;
    public int messageCode = -1;
    
    public Header(){
        super();
    }
    
    public void process(BSEInputStream in) throws Exception{}
}
