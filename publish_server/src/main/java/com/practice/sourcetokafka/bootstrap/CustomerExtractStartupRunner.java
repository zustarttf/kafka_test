package com.practice.sourcetokafka.bootstrap;

import com.practice.sourcetokafka.publish.CustomerPublishService;
import com.practice.sourcetokafka.publish.PublishResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** 기동이 끝나면 {@code src_cust} 전 건을 한 번 발행한다. */
@Slf4j
@Component
@RequiredArgsConstructor
public class CustomerExtractStartupRunner implements ApplicationRunner {

    private final CustomerPublishService customerPublishService;

    /** false면 기동 시에는 발행하지 않고 HTTP 트리거만 남긴다. */
    @Value("${app.extract.on-startup:true}")
    private boolean onStartup;

    /** {@code app.extract.on-startup}이 false면 건너뛴다. */
    @Override
    public void run(ApplicationArguments args) {
        if (!onStartup) {
            log.info("기동 시 발행이 꺼져 있음");
            return;
        }
        PublishResult result = customerPublishService.publishAll();
        log.info(
                "기동 발행 완료. 조회={} 성공={} 실패={}",
                result.getSelectedCount(),
                result.getPublishedCount(),
                result.getFailedCount());
    }
}
