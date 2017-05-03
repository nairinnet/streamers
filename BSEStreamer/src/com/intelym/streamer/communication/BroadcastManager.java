/*
 * Reads the market data broadcast from data source
 */
package com.intelym.streamer.communication;
import com.intelym.streamer.headers.Header;
import com.intelym.streamer.bse.process.ProcessManager;
import java.io.*;
import java.net.*;
/**
 *
 * @author Hari Nair
 * @since Mar 2012
 * @version 1.0
 */
public class BroadcastManager implements Runnable {

    private int broadcastPort = 5000;
    private int broadcastTimeout = 5000;
    private ProcessManager pManager = null;

    private Thread bThread = null;
    private boolean isRunning = false;
    private MulticastSocket dSocket = null;
    private String mCastAddress = null;

    public BroadcastManager(int port, String addr, ProcessManager pm){
        broadcastPort = port;
        pManager = pm;
        mCastAddress = addr;
        bThread = new Thread(this, "BCAST_THREAD");
        isRunning = true;
        bThread.start();
    }

    private void openConnection() throws Exception{
        dSocket = new MulticastSocket(broadcastPort);
        if(mCastAddress != null && mCastAddress.length() > 0) {
            dSocket.joinGroup(InetAddress.getByName(mCastAddress));
        }
        dSocket.setSoTimeout(broadcastTimeout);
    }

    /**
     * run method of the thread, keeps running on while loop and interprets the data
     */
    public void run(){
        
        try{
            openConnection();
            while(isRunning){
                try{
                    receive();
                }catch(Exception e){
                    continue;
                }
            }
        }catch(Exception e){
            isRunning = false;
        }
    }

    /**
     * receives the data over DatagramSocket/Packet.
     * interprets the data according to INORS API.
     * @throws Exception
     */
    private void receive() throws Exception{
        byte buffer[] = new byte[4086];
        DatagramPacket dPacket = new DatagramPacket(buffer, buffer.length);
        dSocket.receive(dPacket);
        ByteArrayInputStream input = new ByteArrayInputStream(dPacket.getData());
        BSEInputStream in = new BSEInputStream(input);
        Header header;
        header = pManager.processHeader(in);
        int length = header.messageLength;
        if(length > 0){
            pManager.processIncoming(in, header);
        }
    }

    public void close(){
        isRunning = false;
        dSocket.close();
    }
}
