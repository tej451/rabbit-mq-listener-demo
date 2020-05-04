package com.example.demo;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.rabbitmq.client.Channel;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Setter
public class RabbitTestListener {


    @Value("${rabbitMQ.test.exchange}")
    private String holdExchange;

    @Value("${rabbitMQ.test.routing.key}")
    private String holdRoutingKey;

    @Value("${rabbitMQ.test.numberOfThreads}")
    private int numberOfThreads;

    private ExecutorService service;

    @PostConstruct
    public void init(){
        service = Executors.newFixedThreadPool(numberOfThreads);
    }


    @RabbitListener(queues = "${rabbitMQ.assortment.request.queue}")
    @Transactional(readOnly = false, rollbackFor = IllegalStateException.class)
    public void onMessage(Message message, Channel channel) throws IOException {
        boolean isAckSuccessful = false;
        Long startTime = System.currentTimeMillis();
        try {
            String messageContent = message.getBody().toString();
            log.info("onMessage: " + messageContent);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            isAckSuccessful = true;
        } catch (IOException e) {
            log.error("onMessage: Failed", e);
        } finally {
            log.info("time taken for request : " + (System.currentTimeMillis() - startTime));
            if (!isAckSuccessful) {
                channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
            }
        }
    }

}
