/**
 * 版权所有(C)，上海海鼎信息工程股份有限公司，2020，所有权利保留。
 * <p>
 * 项目名：	springboot-kafka-demo
 * 文件名：	KafkaProducer.java
 * 模块说明：
 * 修改历史：
 * 2020/11/10 - taozhi - 创建。
 */
package com.snail.springbootkafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author taozhi
 */
@RestController
public class KafkaProducer {

  @Autowired(required = false)
  private KafkaTemplate<String, Object> kafkaTemplate;

  // 发送消息
  @GetMapping("/kafka/normal/topic1/{msg}")
  public void sendMessageTopic1(@PathVariable("msg") String message) {
    kafkaTemplate.send("topic1", message);
  }

  // 发送消息
  @GetMapping("/kafka/normal/topic2/{msg}")
  public void sendMessageTopic2(@PathVariable("msg") String message) {
    for (int i = 0; i < 10; i++) {
      System.out.println("topic2 生产者 发送消息成功：" + i + "："+ message);
      kafkaTemplate.send("topic2", i + "："+ message);
    }
  }

  // 带回调的生产者
  @GetMapping("/kafka/callback/{msg}")
  public void sendMessage2(@PathVariable("msg") String callbackMessage) {
    kafkaTemplate.send("topic1", callbackMessage).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
      @Override
      public void onFailure(Throwable ex) {
        System.out.println("发送消息失败：" + ex.getMessage());
      }

      @Override
      public void onSuccess(SendResult<String, Object> result) {
        System.out.println("发送消息成功：" + result.getRecordMetadata().topic() + "-"
                + result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
      }
    });
  }
}
