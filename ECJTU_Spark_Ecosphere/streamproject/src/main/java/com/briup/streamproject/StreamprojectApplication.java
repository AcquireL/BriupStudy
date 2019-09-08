package com.briup.streamproject;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages ="com.briup.streamproject")
public class StreamprojectApplication {
	public static void main(String[] args) {
		SpringApplication.run(StreamprojectApplication.class, args);
	}
}
