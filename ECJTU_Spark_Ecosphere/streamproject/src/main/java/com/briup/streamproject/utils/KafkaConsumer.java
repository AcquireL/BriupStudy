package com.briup.streamproject.utils;

import com.briup.streamproject.webSocket.WebSocket;
import com.briup.streamproject.webSocket.WebSocketTeacher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 *kafka消费者
 * */
@Component
public class KafkaConsumer {


    /**
     * 实时获取kafka数据(生产一条，监听生产topic自动消费一条)
     * @param record
     * @throws IOException
     */
    @KafkaListener(topicPattern =".*")
    public void listen(ConsumerRecord<?, ?> record) throws IOException {
        String value = (String) record.value();
        String topicName=record.topic();
        if (topicName.equals("teacher")){
            WebSocketTeacher.sendInfo(value);
        }else {
            WebSocket.sendInfo(value,topicName);
        }
    }
}
