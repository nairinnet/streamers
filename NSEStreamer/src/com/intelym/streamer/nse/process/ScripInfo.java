/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.intelym.streamer.nse.process;

/**
 *
 * @author C708587
 */
public class ScripInfo {
    public String   strGroup="",
                    strScripID="";
    public int      iScripID, 
                    iClosePrice,
                    iPerChange;
    public ScripInfo(String exchange, String scripID){
        strScripID = exchange+"_"+scripID;
    }
}
