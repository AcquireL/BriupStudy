package ecjtu.java;

import scala.Int;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class Server implements Runnable{
    public ServerSocket ss;
    public Map<Integer,String> map;

    public void connect(){
        try {
            ss=new ServerSocket (9090);
            Socket socket=ss.accept ();
            InputStream in=socket.getInputStream ();
            ObjectInputStream oin=new ObjectInputStream (in);
            map = (Map<Integer,String>)oin.readObject ();

            for(Map.Entry<Integer,String> entry:map.entrySet ()){
                System.out.println (entry.getKey ());
                System.out.println (entry.getValue ());
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace ();
        }
    }
    @Override
    public void run() {
        connect ();
    }

    public static void main(String[] args) {
        Thread thread=new Thread (new Server ());
        thread.start ();

    }


}
