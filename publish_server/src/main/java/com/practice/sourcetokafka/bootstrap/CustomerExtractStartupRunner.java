package com.practice.sourcetokafka.bootstrap;

import com.practice.sourcetokafka.publish.CustomerPublishService;
import com.practice.sourcetokafka.publish.PublishResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 애플리케이션 기동이 끝난 직후 원천 데이터를 한 번 발행한다.
 *
 * <p>{@link ApplicationRunner}는 컨텍스트와 웹 서버가 올라온 뒤 한 번 실행된다.
 * 토픽을 구독하거나 브로커 연결을 따로 점검하지는 않는다.
 * 연결 확인은 {@link CustomerPublishService#publishAll()}의 첫 전송이 겸한다.
 * Kafka 전송 실패는 서비스 안에서 건별로 잡히므로, 이 메서드까지 예외가 올라오면
 * DB 조회 실패처럼 발행 자체를 시작하지 못한 경우이고 기동은 실패한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CustomerExtractStartupRunner implements ApplicationRunner {

    private final CustomerPublishService customerPublishService;

    /** false면 기동 시에는 뽑지 않고, HTTP 트리거만 남긴다. */
    @Value("${app.extract.on-startup:true}")
    private boolean onStartup;

    /**
     * 기동 발행을 실행하고 건수를 로그로 남긴다.
     *
     * <p>흐름은 {@code onStartup} 확인 후 {@code publishAll()} 한 번이다.
     * 수동 트리거인 {@code POST /api/extract/customers}와 같은 메서드를 호출한다.
     */
    @Override
    public void run(ApplicationArguments args) {
        // app.extract.on-startup이 false면 기동 시 조회와 전송을 건너뛴다.
        // 같은 발행은 POST /api/extract/customers로만 하게 남겨 두려고 여기서 끝낸다.
        if (!onStartup) {
            log.info("Startup extract is disabled");
            return;
        }
        // 수동 트리거와 같은 publishAll이다. DB 조회 실패만 이 메서드 밖으로 나와 기동을 실패시킨다.
        PublishResult result = customerPublishService.publishAll();
        // HTTP 응답이 없는 기동 경로라서, 조회/성공/실패 건수를 로그로만 남긴다.
        log.info(
                "Startup extract finished. selected={} published={} failed={}",
                result.getSelectedCount(),
                result.getPublishedCount(),
                result.getFailedCount());
    }
}
