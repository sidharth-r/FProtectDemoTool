/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpdemo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author fprotect
 */

public class fpdEngine {
    
    Runtime rt;
    Process pZookeeperServer;
    Process pKafkaServer;
    Process pKafkaConnectSource;
    
    String sCmdZkServerStart;
    String sCmdZkServerStop;
    String sCmdKafkaServerStart;
    String sCmdKafkaServerStop;
    String sCmdKafkaConnectStart;
    String sCmdKafkaTopicDeleteBase;
    
    String[] topics;
    String[] topicsInternal;
    String[] checkpointDirs;
    
    boolean deleteTopicsOnShutdown;
    
    InputStream istreamKafkaServer;
    InputStream istreamKafkaServerError;
    InputStream istreamKafkaConnect;
    InputStream istreamKafkaConnectError;
    fpdProcOutWriter writerKafkaServer;
    fpdProcOutWriter writerKafkaServerError;
    fpdProcOutWriter writerKafkaConnect;
    fpdProcOutWriter writerKafkaConnectError;
    
    OutLogger log;
    OutLogger out;
    
    public fpdEngine(Properties props, OutLogger outLog, OutLogger outProc)
    {
        rt = Runtime.getRuntime();
        
        log = outLog;
        log.setAutoRefresh(true);
        out = outProc;
        
        String dt = props.getProperty("kafka.deleteTopicsOnShutdown");
        if(dt.equals("yes"))
            deleteTopicsOnShutdown = true;
        else
            deleteTopicsOnShutdown = false;
        
        String zookeeperPath = props.getProperty("zookeeper.path");
        String kafkaPath = props.getProperty("kafka.path");
        String kafkaConfigPath = props.getProperty("kafka.configPath");
        
        StringBuilder sb = new StringBuilder();
        sb.append(zookeeperPath)
                .append("/bin/zkServer.sh start");
        sCmdZkServerStart = sb.toString();
        
        sb = new StringBuilder().append(zookeeperPath)
                .append("/bin/zkServer.sh stop");
        sCmdZkServerStop = sb.toString();
        
        sb = new StringBuilder().append(kafkaPath)
                .append("/bin/kafka-server-start.sh ")
                .append(kafkaConfigPath)
                .append("/fp-server.properties");
        sCmdKafkaServerStart = sb.toString();
        
        sb = new StringBuilder().append(kafkaPath)
                .append("/bin/kafka-server-stop.sh");
        sCmdKafkaServerStop = sb.toString();
        
        sb = new StringBuilder().append(kafkaPath)
                .append("/bin/connect-standalone.sh ")
                .append(kafkaConfigPath)
                .append("/fp-connect-standalone.properties ")
                .append(kafkaConfigPath)
                .append("/fp-connect-file-source.properties");
        sCmdKafkaConnectStart = sb.toString();
        
        String zkHost = props.getProperty("zookeeper.host");
        String zkPort = props.getProperty("zookeeper.port");
        
        sb = new StringBuilder().append(kafkaPath)
                .append("/bin/kafka-topics.sh --zookeeper ")
                .append(zkHost)
                .append(":")
                .append(zkPort)
                .append(" --delete --topic ");
        sCmdKafkaTopicDeleteBase = sb.toString();

        
        String topicsRaw = props.getProperty("fpdemo.topics") + "," + props.getProperty("fprotect.topics");
        if(topicsRaw != null && !topicsRaw.equals("") && deleteTopicsOnShutdown)
        {
            topics = topicsRaw.split(",");
        }
        
        String topicsIntRaw = props.getProperty("fprotect.topics");
        if(topicsIntRaw != null)
        {
            topicsInternal = topicsIntRaw.split(",");
        }
        
        checkpointDirs = new String[]{props.getProperty("fprotect.spark.checkpoints.dir.prim"),props.getProperty("fprotect.spark.checkpoints.dir.sec")};        
    }
    
    public boolean start() throws IOException, InterruptedException
    {
        log.writeDebug(sCmdZkServerStart);
        pZookeeperServer = rt.exec(sCmdZkServerStart);
        pZookeeperServer.waitFor();
        /*if(!pZookeeperServer.isAlive())
        {
            log.write("Failed to start Zookeeper");
            return false;
        }*/
        
        log.writeDebug(sCmdKafkaServerStart);
        pKafkaServer = rt.exec(sCmdKafkaServerStart);
        writerKafkaServer = new fpdProcOutWriter(pKafkaServer.getInputStream(),out);
        writerKafkaServer.start();
        writerKafkaServerError = new fpdProcOutWriter(pKafkaServer.getErrorStream(),out);
        writerKafkaServerError.start();
        if(!pKafkaServer.isAlive())
        {
            log.write("Failed to start Kafka Server");
            return false;
        }
        Thread.sleep(5000);
        
        log.writeDebug(sCmdKafkaConnectStart);
        pKafkaConnectSource = rt.exec(sCmdKafkaConnectStart);
        writerKafkaConnect = new fpdProcOutWriter(pKafkaConnectSource.getInputStream(),out);
        writerKafkaConnect.start();
        writerKafkaConnectError = new fpdProcOutWriter(pKafkaConnectSource.getErrorStream(),out);
        writerKafkaConnectError.start();
        if(!pKafkaConnectSource.isAlive())
        {
            log.write("Failed to start Kafka Connect");
            return false;
        }
        
        return true;
    }
    
    public void stop() throws IOException, InterruptedException
    {
        writerKafkaServer.quit();
        writerKafkaServerError.quit();
        writerKafkaConnect.quit();
        writerKafkaConnectError.quit();
        
        log.write("Stopping Kafka Connect");
        pKafkaConnectSource.destroy();
        pKafkaConnectSource.waitFor();
        
        if(deleteTopicsOnShutdown)
        {
            log.write("Deleting topics");
            Process p;
            for(int i = 0; i < topics.length; i++)
            {
                p = rt.exec(sCmdKafkaTopicDeleteBase + topics[i]);
                p.waitFor();
                log.write("Deleted topic "+topics[i]);
            }
        }
        
        log.write("Stopping Kafka Server");
        rt.exec(sCmdKafkaServerStop);
        pKafkaServer.waitFor();
        
        log.write("Stopping Zookeeper");
        rt.exec(sCmdZkServerStop);
        pZookeeperServer.waitFor();        
    }
    
    public void restartSys() throws IOException, InterruptedException
    {
        log.write("Deleting internal topics");
        Process p;
        for(int i = 0; i < topicsInternal.length; i++)
        {
            p = rt.exec(sCmdKafkaTopicDeleteBase + topicsInternal[i]);
            p.waitFor();
            log.write("Deleted internal topic "+topicsInternal[i]);
        }
        
        log.write("Clearing Spark checkpoints");
        String cmdClearDirBase = "rm -rf ";
        for (String checkpointDir : checkpointDirs) {
            String cmd = cmdClearDirBase + checkpointDir + "/*";
            log.writeDebug(cmd);
            p = rt.exec(cmd);
            p.waitFor();
        }
    }    
}
