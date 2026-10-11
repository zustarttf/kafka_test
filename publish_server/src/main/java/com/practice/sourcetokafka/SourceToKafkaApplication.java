package com.practice.sourcetokafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 원천 DB를 읽어 표준 메시지로 Kafka에 보내는 앱. */
@SpringBootApplication
public class SourceToKafkaApplication {

    public static void main(String[] args) {
        WindowsConsoleUtf8.enable();
        SpringApplication.run(SourceToKafkaApplication.class, args);
    }
}
