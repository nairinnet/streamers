/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.nse.process;

import com.intelym.streamer.common.Header;
import com.intelym.streamer.communication.NSEInputStream;

/**
 *
 * @author Hari Nair
 */
public interface ProcessManager {
    public Header processHeader(NSEInputStream in) throws Exception;
    public void processIncoming(NSEInputStream in, Header object) throws Exception;
    public void connectionAborted();
}
