package com.practice.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 표준 고객 토픽을 읽어 standard 데이터베이스에 넣는다. */
@SpringBootApplication
public class ConsumerServerApplication {

    public static void main(String[] args) {
        WindowsConsoleUtf8.enable();
        SpringApplication.run(ConsumerServerApplication.class, args);
    }
}
