package ecjtu.java;

import java.sql.*;

public class DBStore {
    public static void main(String[] args) {
        Connection con=null;
        PreparedStatement preparedStatement=null;
        ResultSet resultSet=null;
        try {
            //注册驱动
            Class.forName ("oracle.jdbc.OracleDriver");
            //获得连接
            con = DriverManager.getConnection ("jdbc:oracle:thin:@localhost:1521:XE", "briup", "713181");
            //插入
         /*   PreparedStatement ps=con.prepareStatement(" insert into map values(?,?,?)");
            ps.setInt (1, 9);
            ps.setInt(2, 3);
            ps.setString(3, "ddd");
            ps.executeUpdate();*/
            //查询
            //加载statem对象
            preparedStatement = con.prepareStatement ("select * from map");
            //执行sql
            resultSet = preparedStatement.executeQuery ();
            //处理结果集
            System.out.println (resultSet);
            while (resultSet.next ()){
                //System.out.println ("hello");
                int id = resultSet.getInt("id");
                int key = resultSet.getInt ("key");
                String value = resultSet.getString("value");
                System.out.println(id+"   "+key+"   "+value);
            }
        } catch (Exception e) {
            e.printStackTrace ();
        }finally {
            //关闭资源
            try{
                if(resultSet!=null) resultSet.close ();
                if(preparedStatement!=null) preparedStatement.close ();
                if(con!=null) con.close ();
            }catch (Exception e){
                e.printStackTrace ();
            }

        }
    }
}
