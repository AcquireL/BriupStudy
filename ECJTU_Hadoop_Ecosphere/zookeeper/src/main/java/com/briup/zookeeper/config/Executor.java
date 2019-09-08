package com.briup.zookeeper.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;

public class Executor {
	//表示子进程 即我们写的server程序
	private Process child;
	//子进程要执行的命令 java 类名 ip port
 	private String cmd;
	private ServerConfig cfg;

	public Executor(ServerConfig cfg, String cmd) throws IOException, InterruptedException{
 		this.cmd = cmd;
		this.cfg = cfg;
	}
	public Executor(ServerConfig cfg, 
			Class<?> clazz) throws IOException, InterruptedException{
		this(cfg, "java " + clazz.getName());
	}

	//获得到随机产生的ip和端口
	public String[] getConfig() throws InterruptedException, KeeperException{
		String hostPort = cfg.read(
				ServerConfig.HOST_PORT_CONF, this);
		return hostPort.split(":");
	}
	//获得CLASSPATH环境变量的值
	private String[] getEnv(){
		String cp = System.getProperty(
				"java.class.path");
		return new String[]{"CLASSPATH=" + cp};
	}

	//启动器利用代码，运行服务端程序
	public void runProgram() throws InterruptedException, KeeperException, IOException {
		//随机生成ip地址和端口号
		String[] args = getConfig();
		//构造启动命令
		//java com.briup.zookeeper.Server  host port
		cmd = cmd + " " + args[0] + " " + args[1];
		//构建java虚拟机子进程并加载环境变量
		child = Runtime.getRuntime().exec(cmd, getEnv());
		//启动子进程
		child.waitFor();
	}

	public static void main(String... args) throws IOException, Exception{
		String hosts = "192.168.29.132:2181";
		//连接zookeeper服务器
		ActiveKeyValueStore store = 
				new ActiveKeyValueStore(hosts);
		store.connect();
		//生成用户服务器地址和端口并向zookeeper服务器组册
		ServerConfig cfg = new ServerConfig(store);
		cfg.register();
		//使用cfg的配置信息启动用户的server
		Executor exec = new Executor(
				cfg, Server.class);
		
		exec.runProgram();
	}

	 
}
