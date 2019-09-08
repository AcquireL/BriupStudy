package com.briup.env.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;

import com.briup.env.bean.Environment;

public class GatherImpl implements Gather {
	
	//数据文件路径
	private String dataFilePath;
	public GatherImpl() {
		this.dataFilePath="D:\\idea-hadoop-workpace\\env-gather\\src\\main\\resources\\data-file";
	}
	
	@Override
	public Collection<Environment> gather() throws Exception {
		
		File file=new File(dataFilePath);
		if(!file.exists()) {
			throw new Exception("读取的数据文件路径不存在："+dataFilePath);
		}
		
		Collection<Environment> c=new ArrayList<>();
		
		BufferedReader in=new BufferedReader(new FileReader(file));
		String line = null;
		while((line=in.readLine())!=null) {
			//100|101|2|16|1|3|5d606f7802|1|1516323596029
			String[] arr = line.split("[|]");
			
			if(arr.length!=9) {
				//记录异常数据
				continue;
			}
			Environment env=new Environment();
			env.setSrcId(arr[0]);
			env.setDesId(arr[1]);			
			env.setDevId(arr[2]);
			//表示数据类型：(温度,湿度),二氧化碳,光照强度
			env.setSersorAddress(arr[3]); 
			env.setCount(Integer.parseInt(arr[4]));
			env.setCmd(arr[5]);
			env.setStatus(Integer.parseInt(arr[7]));
			env.setGather_date(new Timestamp(Long.parseLong(arr[8])));
			
			String dataStr= arr[6].substring(0, 4);
			int data = Integer.parseInt(dataStr,16);
			//5d606f7802
			switch (env.getSersorAddress()) {
				case "16":// 温度、湿度
					env.setName("温度");
					env.setData((float)(data*0.00268127-46.85));
					c.add(env);
					//  复制上面封装好的env (温度),使用envCopy表示湿度对象
					Environment envCopy=envCopy(env);
					//  截取数据中的第三四个字节,用来表示湿度数据
					String dataStrHumidity=arr[6].substring(4, 8);
					int dataHumidity=Integer.parseInt(dataStrHumidity,16);
					envCopy.setName("湿度");
					envCopy.setData((float)(dataHumidity*0.00190735-6));
					c.add(envCopy);
					break;
				case "256":// 光照强度
					
					env.setName("光照强度");
					env.setData(data);
					c.add(env);
					break;
				case "1280":// 二氧化碳
					
					env.setName("二氧化碳");
					env.setData(data);
					c.add(env);
					break;
	
				default:
					//数据异常
					break;
			}
			
		}
		in.close();
		return c;
	}

	//复制env对象，注意这里的name和data两个属性没有复制
	private Environment envCopy(Environment env) {
		Environment newEnv=new Environment();
		newEnv.setSrcId(env.getSrcId());
		newEnv.setDesId(env.getDesId());			
		newEnv.setDevId(env.getDevId());
		//表示数据类型：(温度,湿度),二氧化碳,光照强度
		newEnv.setSersorAddress(env.getSersorAddress()); 
		newEnv.setCount(env.getCount());
		newEnv.setCmd(env.getCmd());
		newEnv.setStatus(env.getStatus());
		newEnv.setGather_date(env.getGather_date());
		return newEnv;
	}
	
}
