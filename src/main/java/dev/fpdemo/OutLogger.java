/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpdemo;

import javax.swing.JTextArea;

/**
 *
 * @author fprotect
 */
public class OutLogger{
    JTextArea to;

    OutLogger(JTextArea textOut)
    {
        to = textOut;
    }

    void write(String str)
    {
        to.append(str+"\n");
    }
    
    void writeDebug(String str)
    {
        to.append("DEBUG: "+str+"\n");
    }
}