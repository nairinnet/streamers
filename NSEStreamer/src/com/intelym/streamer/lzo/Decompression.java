/*
 * decompression manager using LZO
 */
package com.intelym.streamer.lzo;

/**
 *
 * @author Hari Nair
 */
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoConstraint;
import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.lzo_uintp;
public class Decompression {
    
    private LzoDecompressor decompressor = null;
    private final lzo_uintp outputBuffLen = new lzo_uintp();
    public Decompression(){
        decompressor = LzoLibrary.getInstance().newDecompressor(LzoAlgorithm.LZO1Z, LzoConstraint.SPEED);
    }
    
    public byte[] decompress(byte[] buffer, int offset, int length, int compressedLength) throws Exception{
        
        ///decompressor.setOutputBufferSize(4084);
        //ecompressor.setInput(buffer, offset, length);
        byte[] outputBuffer = new byte[compressedLength];
        outputBuffLen.value = compressedLength;
        decompressor.decompress(buffer, 0, length, outputBuffer, 0, outputBuffLen);
        return outputBuffer;
    }
}
