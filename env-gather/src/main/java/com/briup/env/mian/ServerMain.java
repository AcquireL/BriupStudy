package com.briup.env.mian;

import com.briup.env.server.Server;
import com.briup.env.server.ServerImpl;
import com.briup.env.server.ServerThreadImpl;

public class ServerMain {
	public static void main(String[] args) {
		Server server=new ServerThreadImpl ();
		
		try {
			server.reciver();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
