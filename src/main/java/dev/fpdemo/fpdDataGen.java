/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 *
 * @author fprotect
 */
public class fpdDataGen {
    
    String sDatasetPath;
    String sTrainSetPath;
    String sTestSetPath;
    float splitRatio;
    
    String sqlConnStr;
    
    OutLogger log;
    
    int nRecords;
    
    public fpdDataGen(Properties props, OutLogger logger, int recordCount)
    {
        String datasetDir = new StringBuilder().append(props.getProperty("fpdemo.dataset.dir"))
                .append("/")
                .toString();
        sDatasetPath = new StringBuilder(datasetDir)
                .append(props.getProperty("fpdemo.dataset.csv"))
                .toString();
        sTrainSetPath = new StringBuilder(datasetDir)
                .append("trainSet.json")
                .toString();
        sTestSetPath = new StringBuilder(datasetDir)
                .append("testSet.json")
                .toString();
        splitRatio = Integer.parseInt(props.getProperty("fpdemo.trainsetPercentage")) / 100.0f;
        
        sqlConnStr = new StringBuilder()
                .append(props.getProperty("mysql.db.url"))
                .append("?")
                .append("user=")
                .append(props.getProperty("mysql.db.user"))
                .append("&password=")
                .append(props.getProperty("mysql.db.pass"))
                .toString();
        
        log = logger;
        
        nRecords = recordCount;
    }
    
    class tr_train
    {
        public int tid;
        public int step;
        public String type;
        public float amount;
        public String nameOrig;
        public float oldBalanceOrig;
        public float newBalanceOrig;
        public String nameDest;
        public float oldBalanceDest;
        public float newBalanceDest;
        public int isFraud;

        public tr_train(int i, int s, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd, int ifr)
        {
            tid = i;
            step = s;
            type = t;
            amount = a;
            nameOrig = no;
            oldBalanceOrig = obo;
            newBalanceOrig = nbo;
            nameDest = nd;
            oldBalanceDest = obd;
            newBalanceDest = nbd;
            isFraud = ifr;
        }
    }
	
    class tr_test
    {
        public int tid;
        public int step;
        public String type;
        public float amount;
        public String nameOrig;
        public float oldBalanceOrig;
        public float newBalanceOrig;
        public String nameDest;
        public float oldBalanceDest;
        public float newBalanceDest;

        public tr_test(int i, int s, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd)
        {
            tid = i;
            step = s;
            type = t;
            amount = a;
            nameOrig = no;
            oldBalanceOrig = obo;
            newBalanceOrig = nbo;
            nameDest = nd;
            oldBalanceDest = obd;
            newBalanceDest = nbd;
        }
    }

    public void run() throws Exception
    {
        Pattern pattern = Pattern.compile(",");
        log.write("Reading dataset "+sDatasetPath);
        BufferedReader in = new BufferedReader(new FileReader(sDatasetPath));
        int tid = 1;

        int n;
        if(nRecords > 0)
            n = nRecords;
        else
            n = count(sDatasetPath);
        int trainLast = (int)(n * splitRatio);
        log.write("Found "+n+" records.");

        String ln;
        ObjectMapper mapper = new ObjectMapper();
        
        int recurrence;
        int destBlacklisted;
        int errorBalanceOrig;
        int errorBalanceDest;
        Connection conn = DriverManager.getConnection(sqlConnStr);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("delete from trhistory");
        stmt.executeUpdate("delete from fpdemo.detection_expected");
        PreparedStatement ps = conn.prepareStatement("insert into trhistory values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        PreparedStatement psl = conn.prepareStatement("insert into fpdemo.detection_expected values(?,?)");
        log.write("Writing training set to mysql://localhost/fprotect/trhistory");
        while(tid <= trainLast)
        {
            ln = in.readLine();
            String[] s = pattern.split(ln);
            tr_train tr = new tr_train(tid,Integer.parseInt(s[0]),s[1],Float.parseFloat(s[2]),s[3],Float.parseFloat(s[4]),Float.parseFloat(s[5]),s[6],Float.parseFloat(s[7]),Float.parseFloat(s[8]),Integer.parseInt(s[9]));
            
            PreparedStatement query = conn.prepareStatement("select count(*) from trhistory where nameOrig = ? and nameDest = ?");
            query.setString(1,tr.nameOrig);
            query.setString(2,tr.nameDest);		
            ResultSet res = query.executeQuery();
            res.first();
            recurrence = res.getInt(res.getMetaData().getColumnName(1));

            query = conn.prepareStatement("select * from blacklist where accountNumber = ?");
            query.setString(1,tr.nameDest);
            res = query.executeQuery();
            res.first();
            destBlacklisted = res.isBeforeFirst() ? 1 : 0;
            
            errorBalanceOrig = (int)(tr.newBalanceOrig + tr.amount - tr.oldBalanceOrig);
            errorBalanceDest = (int)(tr.oldBalanceDest + tr.amount - tr.newBalanceDest);
            
            ps.setInt(1,tr.tid);
            ps.setInt(2,tr.step);
            ps.setString(3,tr.type);
            ps.setFloat(4,tr.amount);
            ps.setString(5,tr.nameOrig);
            ps.setFloat(6,tr.oldBalanceOrig);
            ps.setFloat(7,tr.newBalanceOrig);
            ps.setString(8,tr.nameDest);
            ps.setFloat(9,tr.oldBalanceDest);
            ps.setFloat(10,tr.newBalanceDest);
            ps.setInt(11,recurrence);
            ps.setInt(12,destBlacklisted);
            ps.setInt(13,errorBalanceOrig);
            ps.setInt(14,errorBalanceDest);
            ps.setInt(15, tr.isFraud);
            
            ps.executeUpdate();
            
            psl.setInt(1, tid);
            psl.setInt(2, tr.isFraud);
            psl.executeUpdate();
            
            tid++;
        }
        log.write(("Wrote "+(tid-1)+" records to training set."));
        int ltid = tid;
        
        FileOutputStream testOut = new FileOutputStream(sTestSetPath);
        log.write("Writing test set to "+sTestSetPath);
        while(tid <= n)
        {
            ln = in.readLine();
            String[] s = pattern.split(ln);
            tr_test tr = new tr_test(tid,Integer.parseInt(s[0]),s[1],Float.parseFloat(s[2]),s[3],Float.parseFloat(s[4]),Float.parseFloat(s[5]),s[6],Float.parseFloat(s[7]),Float.parseFloat(s[8]));
            String json = mapper.writeValueAsString(tr) + "\n";
            testOut.write(json.getBytes());
            
            psl.setInt(1, tid);
            psl.setInt(2, Integer.parseInt(s[9]));
            psl.executeUpdate();
            
            tid++;
        }
        testOut.close();
        log.write(("Wrote "+(tid-ltid)+" records to test set."));
        
        conn.close();
        log.write("Wrote labels to evaluation database.");
    }

    int count(String filename) throws IOException 
    {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try 
        {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean endsWithoutNewLine = false;
            while ((readChars = is.read(c)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n')
                        ++count;
                }
                endsWithoutNewLine = (c[readChars - 1] != '\n');
            }
            if(endsWithoutNewLine) {
                ++count;
            } 
            return count;
        } finally 
        {
            is.close();
        }
    }
    
}
