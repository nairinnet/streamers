/*
 * common message object for all the RIWA Publish API Messages, assumed all the specific message class objects
 * needs to extend this class. CommonMessage performs the generic functions for all the messages such as
 * having in byte array and data streams, interpretation messages.
 */

package com.intelym.streamer.headers;
import com.intelym.streamer.communication.BSEInputStream;
import com.intelym.streamer.communication.BSEOutputStream;
import java.io.*;
/**
 *
 * @author Hari Nair
 * @since Mar 2012
 * @version 1.0
 */
public class CommonMessage {

    protected ByteArrayOutputStream bufferOut = null;
    protected BSEOutputStream out = null;

    protected int msgLength = 0;
    /**
     * constructor, initializes the outputstreams to capture the byte buffer
     */
    public CommonMessage(){
        bufferOut = new ByteArrayOutputStream();
        out = new BSEOutputStream(bufferOut);
    }

    public byte[] getBuffer(){
        return bufferOut.toByteArray();
    }

     /**
     * reads the data from the stream as primitive data type values and returns as string
     * @param in MATEInputStream the stream to be read
     * @param index byte, indicates the type of primitive to be read (local config - see constants.java)
     * @return String the values read and interpreted as string
     * @throws IO Exception/ EOF Exception
     */
    public String toString(BSEInputStream in, byte index, boolean divide) throws Exception{
        String tmp = "";
        
        switch(index){
            case Constants.SIGNEDBYTE:
                tmp = in.readByte() + "";
                break;
            case Constants.UNSIGNEDBYTE:
                tmp = in.readUnsignedByte() + "";
                break;
            case Constants.SIGNEDSHORT:
                tmp = (in.readShort()) + "";
                break;
            case Constants.UNSIGNEDSHORT:
                int s = in.readUnsignedShort();
                tmp = (s)+ "";
                break;
            case Constants.SIGNEDINT:
                tmp = (in.readInt())+ "";
                break;
            case Constants.UNSIGNEDINT:
                tmp = ((in.readInt() & 0xFFFFFFFFL))+ "";
                break;
            case Constants.SIGNEDLONG:
                tmp = (in.readLong()) + "";
                break;
            case Constants.SIGNEDFLOAT:
                tmp = in.readDouble() + "";
                break;
            default:
                break;
        }
        return tmp;
    }

    public byte readByte(BSEInputStream in) throws Exception {
        return in.readByte();
    }
    
    public short readShort(BSEInputStream in) throws Exception {
        return in.readShort();
    }
    
    public short readUnsignedShort(BSEInputStream in) throws Exception {
        return in.readShort();
    }
    
    public int readInt(BSEInputStream in) throws Exception {
        return in.readInt();
    }
    
    public long readUnsignedInt(BSEInputStream in) throws Exception {
        return (in.readInt() & 0xFFFFFFFFL);
    }
    
    public long readLong(BSEInputStream in) throws Exception {
        return in.readLong();
    }
    
    
    public double readDouble(BSEInputStream in) throws Exception {
        return in.readDouble();
    }
    
    public void skipBytes(BSEInputStream in, int noOfBytes) throws Exception {
        byte[] bytesTobeSkipped = new byte[noOfBytes];
        in.read(bytesTobeSkipped);
    }
    
    /**
    * reads the data from the stream as chars for specific length and returns as string
    * @param in MATEInputStream the stream to be read
    * @param length of the chars to be read
    * @return String the values read and interpreted as string
    * @throws IO Exception/EOF Exception
    */
    public String toChars(BSEInputStream in, int length) throws Exception{
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

    /**
    *
    * writes the single byte character into byte stream, typically written to avoid java's two byte character
    * confusion. use it wisely
    * @param len length of the character array to be written into bytearray
    * @param value to be written
    * @throws Exception null pointer? could be
    */
    public void writeChars(int len, String value) throws Exception{
        int tmp = value.length();
        for(int i = 0; i < len; i++){
            if(i < tmp)
                out.writeByte(value.charAt(i));
            else
                out.writeByte(0x00);
        }
    }

    public int getLength() {
        return msgLength;
    }

}


