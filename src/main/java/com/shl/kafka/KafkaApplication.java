package com.shl.kafka;

import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@SpringBootApplication
@RequestMapping
class KafkaApplication {

  private static final Logger logger = LoggerFactory.getLogger(KafkaApplication.class);

  @Resource
  private KafkaTemplate template;

  private static final String topic = "topictest";

  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }

  @RequestMapping("/index")
  public String index() {
    return "hello kafka";
  }

  /**
   * 发送消息
   */
  @GetMapping("/send/{input}")
  public String send(@PathVariable String input) {
    this.template.send(topic, input);
    return "send success";
  }

  /**
   * 消费消息
   */
  @KafkaListener(id = "", topics = topic, groupId = "group.demo")
  public void listener(String input) {
    logger.info("input value：{}", input);
  }

  /**
   * 发送消息：使用事务，方式1 template#executeInTransaction
   */
  @GetMapping("/send/transaction/{input}")
  public String sendTransaction(@PathVariable String input)
      throws ExecutionException, InterruptedException {
    // 事务操作
    template.executeInTransaction(t -> {
      t.send(topic, input);
      if ("error".equals(input)) {
        throw new RuntimeException("input is error");
      }
      t.send(topic, input + " anthor");
      return true;
    });

    return "send success";
  }

  /**
   * 发送消息：使用事务，方式2 @Transactional(rollbackFor = RuntimeException.class)
   */
  @GetMapping("/sendt/transaction/{input}")
  @Transactional(rollbackFor = RuntimeException.class)
  public String sendTransaction2(@PathVariable String input)
      throws ExecutionException, InterruptedException {
    template.send(topic, input);
    if ("error".equals(input)) {
      throw new RuntimeException("input is error");
    }
    template.send(topic, input + " anthor");
    return "send success";
  }
}
