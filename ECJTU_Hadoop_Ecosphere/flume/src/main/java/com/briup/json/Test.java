package com.briup.json;

import com.google.gson.Gson;
import net.sf.json.JSONObject;
import org.apache.flume.event.JSONEvent;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {
	public static void main(String[] args) throws Exception {
		try{
			//创建连接
			URL url = new URL("http://192.168.29.131:8888");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod("POST");
			connection.setUseCaches(false);
			connection.setInstanceFollowRedirects(true);
			connection.setRequestProperty("Content-Type",
					"application/x-www-form-urlencoded");
			connection.connect();
			//POST请求
			DataOutputStream out = new DataOutputStream(
					connection.getOutputStream());

			JSONEvent jse = new JSONEvent();
			Map ipt = new HashMap();
			ipt.put("type", "g3");
			ipt.put("brand", "six god");
			jse.setBody("cc33  test".getBytes());
			jse.setHeaders(ipt);
			Gson gson = new Gson();
			List events1 = new ArrayList();
			events1.add(jse);
			out.writeBytes(gson.toJson(events1));
			out.flush();
			out.close();

			//读取响应
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			String lines;
			StringBuffer sb = new StringBuffer("");
			while ((lines = reader.readLine()) != null) {
				lines = new String(lines.getBytes(), "utf-8");
				sb.append(lines);
			}
			System.out.println(sb);
			reader.close();
			// 断开连接
			connection.disconnect();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}


