package com.briup.env.server;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;

import com.briup.env.bean.Environment;

public class ServerImpl implements Server{
	
	private int port=8888;
	
	

	@Override
	public void reciver() throws Exception {
		ServerSocket server=new ServerSocket(port);
		Socket socket = server.accept();
		
		//socket.getInputStream() 获取基于网络客服端的字节输入流
		//并把该字节输入流包装成对象，用来读取客户端发送过来的集合
		ObjectInputStream in=new ObjectInputStream(socket.getInputStream());
		
		@SuppressWarnings("unchecked")
		Collection<Environment> c=(Collection<Environment>)in.readObject();
		
		System.out.println("服务器接收到客服端发送到的数据"+c.size()+"条。");
		in.close();
		socket.close();
		server.close();
	}

	@Override
	public void shutdown() throws Exception {
		
	}

}
