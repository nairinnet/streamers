/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.communication.nse;

import com.intelym.streamer.lzo.Decompression;
import com.intelym.streamer.nse.process.ProcessManager;
import com.intelym.logger.LoggerFactory;
import com.intelym.logger.RiwaLogger;
import com.intelym.streamer.common.Header;
import com.intelym.streamer.communication.NSEInputStream;
import java.io.ByteArrayInputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 *
 * @author Hari Nair
 * @since Oct 2014
 * @ver 1.0
 */
public class BroadcastManager implements Runnable{
    
    private static RiwaLogger mLog = LoggerFactory.getLogger(BroadcastManager.class);
    private int broadcastPort = 5000;
    private int broadcastTimeout = 5000;
    private ProcessManager pManager = null;

    private Thread bThread = null;
    private boolean isRunning = false;
    private MulticastSocket dSocket = null;
    private String mCastAddress = null;
    Decompression decomp = new Decompression();
    
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
    @Override
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
  
    public String toChars(NSEInputStream in, int length) throws Exception{
        String tmp = "";
        boolean identifiedNullTermination = false;
        for(int i = 0; i < length; i++){
            byte b = (byte) in.readByte();
            if(b == 0x00){
                identifiedNullTermination = true;
            }
            if(!identifiedNullTermination)
                tmp += String.valueOf((char)b);
        }
        return tmp;
    }

     
    public NSEInputStream unPack(byte[] buffer) throws Exception{
        try{
            ByteArrayInputStream input = new ByteArrayInputStream(buffer);
            NSEInputStream in = new NSEInputStream(input);
            String netId = toChars(in, 2);
            int noOfPackets = in.readShort();
            int lenOfCompressedPackets = in.readShort();
            

            byte[] outputBuffer;
            //int lenOfCompressedPackets = 512;
            if(lenOfCompressedPackets > 0){
                byte[] compressedBuffer = new byte[506];
                in.readFully(compressedBuffer, 0, compressedBuffer.length);
                
                outputBuffer = decomp.decompress(compressedBuffer, 0, compressedBuffer.length, 1024 * 10);
                ByteArrayInputStream newInput = new ByteArrayInputStream(outputBuffer);
                NSEInputStream newIn = new NSEInputStream(newInput);
                return newIn;
            }
            else{
                
                return in;
            }
            
            
        }catch(Exception e){
            return null;
        }
    }
    
    /**
     * receives the data over DatagramSocket/Packet.
     * interprets the data according to INORS API.
     * @throws Exception
     */
    private void receive() throws Exception{
        
        byte buffer[] = new byte[512];
        DatagramPacket dPacket = new DatagramPacket(buffer, buffer.length);
        dSocket.receive(dPacket);

        NSEInputStream in = unPack(dPacket.getData());
        if(in != null){
            
           
            Header header;
            in.skipBytes(8);
            header = pManager.processHeader(in);
            int length = header.messageLength;
            if(length > 0){
                pManager.processIncoming(in, header);
            }
        }

    }

    public void close(){
        isRunning = false;
        dSocket.close();
    }
}
