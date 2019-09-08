package com.briup.zookeeper.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

public class Server extends Thread{
	private boolean stop = false;
	private Socket socket;
	private static ServerSocket ss;

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	public Server(Socket socket) {
		this.socket = socket;
	}

	public static void main(String[] args) {
		try {
			ss = new ServerSocket();
			SocketAddress sa = new InetSocketAddress(
					args[0], Integer.parseInt(args[1]));
			ss.bind(sa);
			Socket socket = ss.accept();
			new Server(socket).start();
		} catch (IOException e) {
 			e.printStackTrace();
		}
	}

	@Override
	public void run() {
 		try {
			BufferedReader br = 
				new BufferedReader(
					new InputStreamReader(
						socket.getInputStream()));
			PrintWriter pw = 
				new PrintWriter(socket.getOutputStream(), true);
			while(!stop){
				pw.println(br.readLine());
			}
		} catch (IOException e) {
 			e.printStackTrace();
		}
	}

}
