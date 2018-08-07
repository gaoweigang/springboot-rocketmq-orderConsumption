package com.gwg.demo.mq.service;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.Application;
import com.gwg.demo.model.User;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=Application.class)
public class RocketMQTest {
	
	private static final Logger logger = LoggerFactory.getLogger(RocketMQTest.class);
	
	@Autowired
	private DefaultMQProducer producer;
	
	/**
	 * 相同订单号的--->有相同的模--->有相同的queue。
	 */
	@Test
	public void testProduceMessage() throws MQClientException, RemotingException, MQBrokerException, InterruptedException, UnsupportedEncodingException{
	   
	  for(int orderNo = 0; orderNo< 10 ; orderNo++){
		   User user = new User(orderNo, "gaoweigang", new Date());
			 
	       Message message = new Message("TopicTest", "tagA", JSON.toJSONString(user).getBytes("UTF-8"));
			
	       SendResult result = producer.send(message, new MessageQueueSelector() {
			
			@Override
			public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
				logger.info("写入队列的大小：{}", mqs.size());
			    Integer id = (Integer) arg;
			    int index = id / mqs.size();//如果master宕机，会导致写入队列mqs数量上出现变化，就可能导致乱序
				return mqs.get(index);
			}
		   }, orderNo);
	       logger.info("发送响应：MsgId:" + result.getMsgId() + "，发送状态:" + result.getSendStatus());
	  }
		
		
	}	
	

}
