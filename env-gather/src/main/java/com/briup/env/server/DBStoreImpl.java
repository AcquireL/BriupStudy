package com.briup.env.server;

import com.briup.env.bean.Environment;
import com.briup.env.client.Gather;
import com.briup.env.client.GatherImpl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;

public class DBStoreImpl implements DBStore {
    private String driver="oracle.jdbc.driver.OracleDriver";
    private String url="jdbc:oracle:thin:@127.0.0.1:1521:XE ";
    private String name="briup";
    private String password="713181";

    @Override
    public void saveDB(Collection<Environment> c) throws Exception {
        Class.forName(driver);
        Connection  conn=DriverManager.getConnection(url,name,password );

        PreparedStatement ps=null;
        String sql=null;
        int day=-1;
        int count=0;

        conn.setAutoCommit (false);
        for(Environment e:c){
            if(day!=e.getGather_date ().getDate ()){
                if(ps!=null){
                    ps.executeBatch ();
                    ps.close ();
                    count=0;
                }
                day=e.getGather_date ().getDate ();
                sql="insert into e_detail_"+day+"(name,srcId,desId,devId,sersorAddress,count,cmd,status,data,gather_date) values (?,?,?,?,?,?,?,?,?,?)";
                ps=conn.prepareStatement(sql);
            }
            ps.setString(1, e.getName());
            ps.setString(2, e.getSrcId());
            ps.setString(3, e.getDesId());
            ps.setString(4, e.getDevId());
            ps.setString(5, e.getSersorAddress());
            ps.setInt(6, e.getCount());
            ps.setString(7, e.getCmd());
            ps.setInt(8, e.getStatus());
            ps.setFloat(9, e.getData());
            ps.setTimestamp(10, e.getGather_date());

            ps.addBatch();
            count++;

            if(count==50){
                ps.executeBatch ();
                count=0;
            }
        }
        if(ps!=null){
            ps.executeBatch ();
            ps.close ();
        }
        conn.commit ();
        if(conn!=null){
            conn.close ();
        }
    }
}
