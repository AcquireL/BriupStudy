package com.briup.streamproject.webSocket;


import org.springframework.stereotype.Component;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 老师的WebSocket
 * */
@Component
@ServerEndpoint("/wbSocketTeacher")
public class WebSocketTeacher {

    private Session session;
    //此处定义静态变量，以在其他方法中获取到所有连接
    public static CopyOnWriteArraySet<WebSocketTeacher> wbSockets = new CopyOnWriteArraySet<WebSocketTeacher>();



    /**
     * 建立连接。
     * 建立连接时入参为session
     */
    @OnOpen
    public void onOpen(Session session){
        this.session = session;
        //将此对象存入集合中以在之后广播用，如果要实现一对一订阅，则类型对应为Map。由于这里广播就可以了随意用Set
        wbSockets.add(this);
    }
    /**
     * 关闭连接
     */
    @OnClose
    public void onClose(){
        //将socket对象从集合中移除，以便广播时不发送次连接。如果不移除会报错(需要测试)
        wbSockets.remove(this);
    }
    /**
     * 接收前端传过来的数据。
     */
    @OnMessage
    public void onMessage(String loginName ,Session session){
        try {
            sendInfo(loginName);
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
     * */
    public  static void sendInfo(String message) throws IOException {
        for (WebSocketTeacher socketTeacher : wbSockets) {
            try {
                socketTeacher.sendMessage(message);
            } catch (IOException e) {
                continue;
            }
        }
    }
}
