package com.practice.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 표준 고객 토픽을 읽어 {@code standard} 데이터베이스에 적재하는 앱.
 *
 * <p>기동하면 컨슈머 그룹 {@code customer-snapshot-consumer}로 브로커에 읽기를 요청한다.
 * 그룹에 저장된 오프셋이 없으면 남아 있는 첫 메시지부터 읽고, 있으면 그 다음부터 읽는다.
 */
@SpringBootApplication
public class ConsumerServerApplication {

    public static void main(String[] args) {
        // 컨슈머 로그의 한글 이름보다 먼저 콘솔을 UTF-8로 맞춘다.
        WindowsConsoleUtf8.enable();
        // 컨텍스트가 올라가면 KafkaListener가 그룹으로 구독을 시작한다. HTTP 포트는 없다.
        SpringApplication.run(ConsumerServerApplication.class, args);
    }
}
