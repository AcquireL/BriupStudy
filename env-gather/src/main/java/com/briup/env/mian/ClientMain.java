package com.briup.env.mian;

import java.util.Collection;

import com.briup.env.bean.Environment;
import com.briup.env.client.Client;
import com.briup.env.client.ClientImpl;
import com.briup.env.client.Gather;
import com.briup.env.client.GatherImpl;

public class ClientMain {
	public static void main(String[] args) {
		
		Gather g=new GatherImpl();
		Client client=new ClientImpl();
		
		try {
			Collection<Environment> c = g.gather();
			client.send(c);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
