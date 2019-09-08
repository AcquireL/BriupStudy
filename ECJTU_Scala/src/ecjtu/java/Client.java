package ecjtu.java;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Client {
    public static void getConnection(){
        try {
            Map<Integer,String> map = new HashMap<> ();
            map.put (1, "test");
            Socket socket=new Socket ("127.0.0.1",9090);
            OutputStream out=socket.getOutputStream ();
            ObjectOutputStream oout=new ObjectOutputStream (out);
            oout.writeObject (map);
        } catch (IOException e) {
            e.printStackTrace ();
        }

    }
    public static void main(String[] args) {
        getConnection ();
    }
}
