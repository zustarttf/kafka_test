package com.practice.sourcetokafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 원천 DB를 읽어 표준 메시지로 바꾼 뒤 Kafka로 보내는 앱의 시작점.
 *
 * <p>기동 순서는 다음과 같다.
 * <ol>
 *   <li>{@code application.yml}로 PostgreSQL 데이터소스와 {@code KafkaTemplate}을 만든다.
 *       이 시점에는 Kafka 브로커에 접속하지 않는다.</li>
 *   <li>{@code schema.sql}, {@code data.sql}로 {@code src_cust}를 준비한다.
 *       DB 접속이 실패하면 여기서 기동이 멈춘다.</li>
 *   <li>빈이 준비된 뒤 {@link com.practice.sourcetokafka.bootstrap.CustomerExtractStartupRunner}가
 *       테이블 전 건을 발행한다. 브로커가 꺼져 있으면 전송만 실패하고 프로세스는 남는다.</li>
 * </ol>
 * 같은 발행은 {@code POST /api/extract/customers}로 다시 실행할 수 있다.
 */
@SpringBootApplication
public class SourceToKafkaApplication {

    /**
     * 스프링 컨테이너를 올리고, 기동 러너까지 포함한 위의 순서를 시작한다.
     */
    public static void main(String[] args) {
        // 로그를 찍기 전에 콘솔 코드 페이지를 UTF-8로 맞춘다. 스프링이 뜬 뒤에 바꾸면 앞선 한글이 깨질 수 있다.
        WindowsConsoleUtf8.enable();
        // 이 호출이 yml, DB 초기화, 기동 발행까지 진행한다. Kafka 접속은 발행 시점까지 미룬다.
        SpringApplication.run(SourceToKafkaApplication.class, args);
    }
}
