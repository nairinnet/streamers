/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.headers;

/**
 *
 * @author Hari Nair
 */
public class Constants {
     public static final byte SIGNEDBYTE = 1,
                            UNSIGNEDBYTE = 2,
                            SIGNEDSHORT = 3,
                            UNSIGNEDSHORT = 4,
                            SIGNEDINT = 5,
                            UNSIGNEDINT = 6,
                            SIGNEDLONG = 7,
                            SIGNEDFLOAT = 8;
     
     public static final byte TCP = 1,
                            WEBSOCKET = 2;
}
