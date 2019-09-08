package com.briup.env.test;

import com.briup.env.bean.Environment;
import com.briup.env.client.Gather;
import com.briup.env.client.GatherImpl;
import com.briup.env.server.DBStore;
import com.briup.env.server.DBStoreImpl;
import sun.security.pkcs11.Secmod;

import java.util.Collection;

public class HelloTest {
	public static void main(String[] args) throws Exception {
		
		//System.out.println("hello world");

	/*	DBStore dbStore=new DBStoreImpl();
		try{
			dbStore.saveDB (null);
		}catch (Exception e){
			e.printStackTrace ();
		}*/
		Gather g = new GatherImpl ();
		Collection<Environment> c = g.gather ();
		System.out.println (c.size ());
		for (Environment e:c){
			System.out.println (e.toString ());
		}
}
}
