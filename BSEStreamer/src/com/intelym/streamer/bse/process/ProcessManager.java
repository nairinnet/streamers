/*
 * Event interface for the process manager, to be called from the classes, which requires the access of
 * Processor
 * @see SocketReader.java
 */

package com.intelym.streamer.bse.process;
import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.headers.Header;

/**
 *
 * @author Hari Nair
 * @since Jul 2013
 * @version 1.1
 */
public interface ProcessManager {
    public Header processHeader(BSEInputStream in) throws Exception;
    public void processIncoming(BSEInputStream in, Header object) throws Exception;
    public void connectionAborted();
}
