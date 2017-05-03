/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.data;

/**
 *
 * @author Hari Nair
 * Modified by Nirmal on 09-03-2016 for time based feeds
 */
 public class ScripData{
        
    public long timeInMillis;
    public int scripCode;
    public int lastTradedPrice;
    public int prevClosePrice;
    public int tradedVolume;
    public int previousLTP;
    public int percentagechane;
    public boolean alreadySend = false;
}