package com.briup.streamproject.webSocket;

import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.springframework.stereotype.Component;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 学生的webSocket
 * */
@Component
@ServerEndpoint("/wbSocket")
public class WebSocket {

    public WebSocket(){

    }
    private Session session;

    //此处定义静态变量，以在其他方法中获取到所有连接
    public static CopyOnWriteMap<String, CopyOnWriteArraySet<WebSocket>> wbSockets = new CopyOnWriteMap<>();

    private  String key;


    /**
     * 建立连接。
     * 建立连接时入参为session
     */
    @OnOpen
    public void onOpen(Session session){
        this.session = session;
        //将此对象存入集合中以在之后广播用，如果要实现一对一订阅，则类型对应为Map。由于这里广播就可以了随意用Set
//      wbSockets.add(this);
    }
    /**
     * 关闭连接
     */
    @OnClose
    public void onClose(){
        //将socket对象从集合中移除，以便广播时不发送次连接。如果不移除会报错(需要测试)
        if(wbSockets.containsKey(key)){
            CopyOnWriteArraySet<WebSocket> set=wbSockets.get(key);
            set.remove(this);
        }
    }
    /**
     * 接收前端传过来的数据。
     */
    @OnMessage
    public void onMessage(String loginName ,Session session){
        key=loginName;
        //添加到集合中
        CopyOnWriteArraySet<WebSocket> set=null;
        //更新值
        if(wbSockets.containsKey(loginName)){
            set=wbSockets.get(loginName);
        }else{
            //第一次出现
            set=new CopyOnWriteArraySet<>();
        }
        set.add(this);
        //赋值
        wbSockets.put(loginName,set);
        try {
            /**
             * 学生对象也可会登陆多个账户，因此也是群发。
             * */
            sendInfo(loginName,loginName);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 单发消息
     * */
    public void sendMessage(String message) throws IOException {
        this.session.getBasicRemote().sendText(message);
    }

    /**
     * 群发自定义消息
     * @param  message 消息
     * */
    public static void sendInfo(String message,String topicName) throws IOException {
        for (String key1 : wbSockets.keySet()) {
            if(key1.equals(topicName)){
                try {
                    CopyOnWriteArraySet<WebSocket> webSocket=wbSockets.get(key1);
                    if(webSocket!=null){
                        for(WebSocket socket:webSocket){
                            socket.sendMessage(message);
                        }
                    }
                } catch (IOException e) {
                    continue;
                }
            }
        }
    }

}
