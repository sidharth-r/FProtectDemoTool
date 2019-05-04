/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpdemo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JTextArea;

/**
 *
 * @author fprotect
 */
public class fpDemoTool extends javax.swing.JFrame {

    Properties props;
    fpdInStreamFileSource streamSource;
    
    /**
     * Creates new form fpDemoTool
     */
    public fpDemoTool() throws IOException {
        initComponents();
        
        buttonStop.setEnabled(false);
        
        OutLogger outLog = new OutLogger(textLog);
        OutLogger outProc = new OutLogger(textOut);
        
        props = new Properties();
        InputStream in = this.getClass().getResourceAsStream("/fpDemoTool.properties");
        props.load(in);
        
        File propsFile = new File("fpDemoTool.properties");
        if(propsFile.isFile())
        {
            in = new FileInputStream(propsFile);
            props.load(in);
        }
        else
            props.store(new FileOutputStream("fpDemoTool.properties"), null);     
        
        streamSource = new fpdInStreamFileSource(props, outLog, outProc);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jScrollPane1 = new javax.swing.JScrollPane();
        textOut = new javax.swing.JTextArea();
        buttonStart = new javax.swing.JButton();
        buttonStop = new javax.swing.JButton();
        jScrollPane2 = new javax.swing.JScrollPane();
        textLog = new javax.swing.JTextArea();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        textOut.setEditable(false);
        textOut.setColumns(20);
        textOut.setRows(5);
        jScrollPane1.setViewportView(textOut);

        buttonStart.setText("Start");
        buttonStart.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                buttonStartMouseClicked(evt);
            }
        });

        buttonStop.setText("Stop");
        buttonStop.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                buttonStopMouseClicked(evt);
            }
        });

        textLog.setEditable(false);
        textLog.setColumns(20);
        textLog.setRows(5);
        jScrollPane2.setViewportView(textLog);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(layout.createSequentialGroup()
                        .addContainerGap()
                        .addComponent(jScrollPane2))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(27, 27, 27)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(buttonStart)
                            .addComponent(buttonStop))
                        .addGap(192, 192, 192)
                        .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 763, Short.MAX_VALUE)))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(31, 31, 31)
                        .addComponent(buttonStart)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(buttonStop)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 287, Short.MAX_VALUE))
                    .addGroup(layout.createSequentialGroup()
                        .addContainerGap()
                        .addComponent(jScrollPane1)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)))
                .addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void buttonStartMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_buttonStartMouseClicked
        try {
            if(streamSource.start())
            {
                textOut.append("Started source stream.\n");
                buttonStart.setEnabled(false);
                buttonStop.setEnabled(true);
            }
            else
                textOut.append("Failed to start source stream.\n");
        } catch (IOException ex) {
            Logger.getLogger(fpDemoTool.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(fpDemoTool.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_buttonStartMouseClicked

    private void buttonStopMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_buttonStopMouseClicked
        try {
            streamSource.stop();
            buttonStop.setEnabled(false);
            buttonStart.setEnabled(true);
        } catch (IOException ex) {
            Logger.getLogger(fpDemoTool.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(fpDemoTool.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_buttonStopMouseClicked

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(fpDemoTool.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(fpDemoTool.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(fpDemoTool.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(fpDemoTool.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                try {
                    new fpDemoTool().setVisible(true);
                } catch (IOException ex) {
                    Logger.getLogger(fpDemoTool.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton buttonStart;
    private javax.swing.JButton buttonStop;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JTextArea textLog;
    private javax.swing.JTextArea textOut;
    // End of variables declaration//GEN-END:variables
}
