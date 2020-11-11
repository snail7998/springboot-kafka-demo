/**
 * 版权所有(C)，上海海鼎信息工程股份有限公司，2020，所有权利保留。
 * <p>
 * 项目名：	springboot-kafka-demo
 * 文件名：	KafkaConsumer.java
 * 模块说明：
 * 修改历史：
 * 2020/11/10 - taozhi - 创建。
 */
package com.snail.springbootkafkademo.controller;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author taozhi
 */
@Configuration
@EnableKafka
public class KafkaConsumer {

  @Value("${spring.kafka.bootstrap-servers}")
  private String service;

  @Value("${spring.kafka.consumer.properties.group.id}")
  private String groupid;

  @Value("${spring.kafka.consumer.enable-auto-commit}")
  private String autoCommit;

  @Value("${spring.kafka.consumer.auto.commit.interval.ms}")
  private String interval;

  // 默认发送心跳时间为10000ms，超时时间需要大于发送心跳时间
  @Value("10000")
  private String timeout;

  @Value("${spring.kafka.consumer.key-deserializer}")
  private String keyDeserializer;

  @Value("${spring.kafka.consumer.value-deserializer}")
  private String valueDeserializer;

  @Value("${spring.kafka.consumer.auto-offset-reset}")
  private String offsetReset;


  @Autowired
  ConsumerFactory consumerFactory;

  //消费监听
  // @KafkaListener(topics = {"topic1"})
  public void onMessage(ConsumerRecord<?, ?> record) {
    // 消费的哪个topic、 partition的消息、消息内容
    System.out.println("简单消费：" + record.topic() + "---" + record.partition() + "---" + record.value());
  }


  /**
   * @return void
   * @Title 指定topic、partition、offset消费
   * @Description 同时监听topic1和topic2，监听topic1的0号分区、topic2的 "0号和1号" 分区，指向1号分区的offset初始值为8
   * @Author long.yuan
   * @Date 2020/3/22 13:38
   * @Param [record]
   **/
/*  @KafkaListener(id = "consumer1", groupId = "felix-group", topicPartitions = {
          @TopicPartition(topic = "topic1", partitions = {"0"}),
          @TopicPartition(topic = "topic2", partitions = "0", partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "1"))
  })*/
  public void onMessage2(ConsumerRecord<?, ?> record) {
    System.out.println("topic:" + record.topic() + "|partition:" + record.partition() + "|offset:" + record.offset() + "|value:" + record.value());
  }

  /**
   * 批量消费+自定义异常处理器
   * <p>
   * 只要不更改group.id，每次重新消费kafka，都是从上次消费结束的地方继续开始，不论"auto.offset.reset”属性设置的是什么
   *
   * @param recordList
   * @throws Exception
   */
  // @KafkaListener(id = "consumer2",groupId = "felix-group", topics = "topic2", errorHandler = "consumerAwareErrorHandler")
  public void onMessage3(List<ConsumerRecord<?, ?>> recordList) throws Exception {
    System.out.println("批量消费一次：" + recordList.size());
    for (ConsumerRecord<?, ?> consumerRecord : recordList) {
      System.out.println(consumerRecord.value());
      throw new Exception("批量消费，模拟异常情况");
    }
  }


  // 新建一个异常处理器，用@Bean注入
  @Bean
  public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
    return (message, exception, consumer) -> {
      System.out.println("消费异常：" + message.getPayload());
      return null;
    };
  }

  // 消息过滤器
  //@Bean
  public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
    ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
    factory.setConsumerFactory(consumerFactory);
    // 被过滤的消息将被丢弃
    factory.setAckDiscarded(true);
    // 消息过滤策略，奇数过滤
    factory.setRecordFilterStrategy(consumerRecord -> {
      if (Integer.parseInt(consumerRecord.value().toString()) % 2 == 0) {
        return false;
      }
      //返回true消息则被过滤
      return true;
    });
    return factory;
  }

  // 消息过滤监听
  //@KafkaListener(id = "consumer3",groupId = "felix-group", topics = "topic2", containerFactory = "filterContainerFactory")
  public void onMessage4(List<ConsumerRecord<?, ?>> recordList) {
    System.out.println(recordList.get(0).value());
  }

  /**
   * 批量消费+并发消费+手动提交
   *
   * @param records
   */
  @KafkaListener(/*id = "consumer2", groupId = "felix-group", */topics = "topic2", containerFactory = "kafkaListenerContainerFactory2")
  public void onMessage5(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
    System.out.println("BBB此线程消费" + records.size() + "条消息----线程名:" + Thread.currentThread().getName());
    records.forEach(record -> System.out.println("topic名称:" + record.topic() + "---" + "分区位置:" + record.partition() + "---" + "key:" + record.key() + "---" + "偏移量:" + record.offset() + "---" + "消息内容:" + record.value()));
    ack.acknowledge();
  }

  @KafkaListener(/*id = "consumer2", groupId = "felix-group", */topics = "topic2", containerFactory = "kafkaListenerContainerFactory2")
  public void onMessage6(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
    System.out.println("AAA此线程消费" + records.size() + "条消息----线程名:" + Thread.currentThread().getName());
    records.forEach(record -> System.out.println("topic名称:" + record.topic() + "---" + "分区位置:" + record.partition() + "---" + "key:" + record.key() + "---" + "偏移量:" + record.offset() + "---" + "消息内容:" + record.value()));
    ack.acknowledge();
  }


  /**
   * 获取kafka实例，该实例为批量消费
   * @return kafka实例
   */
  @Bean(name = "kafkaListenerContainerFactory2")
  public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,String>> kafkaListenerContainerFactory2(){
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setConcurrency(2);  // 连接池中消费者数量
    factory.setBatchListener(true); // 是否并发消费
    factory.getContainerProperties().setPollTimeout(4000); //拉取topic的超时时间
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);// 关闭ack自动提交偏移
    return factory;
  }

  /**
   * 获取工厂
   * @return kafka工厂
   */
  private ConsumerFactory<String,String> consumerFactory(){
    Map<String, Object> props = consumerConfig();
    // 日志过滤入库一批量为1500条消息
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1500);// 单次poll的数量,批量消费时配置
    return new DefaultKafkaConsumerFactory<>(consumerConfig());
  }

  /**
   * 获取kafka配置
   * @return 配置map
   */
  private Map<String,Object> consumerConfig(){
    Map<String,Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,service);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, interval);
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,timeout);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetReset);
    return props;
  }


}
