package com.practice.sourcetokafka.bootstrap;

import com.practice.sourcetokafka.publish.AccountPublishService;
import com.practice.sourcetokafka.publish.CustomerPublishService;
import com.practice.sourcetokafka.publish.ProductPublishService;
import com.practice.sourcetokafka.publish.PublishResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** 기동이 끝나면 고객, 계좌, 상품 스냅샷을 토픽마다 한 번씩 발행한다. */
@Slf4j
@Component
@RequiredArgsConstructor
public class CustomerExtractStartupRunner implements ApplicationRunner {

    private final CustomerPublishService customerPublishService;
    private final AccountPublishService accountPublishService;
    private final ProductPublishService productPublishService;

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
        logResult(customerPublishService.publishAll());
        logResult(accountPublishService.publishAll());
        logResult(productPublishService.publishAll());
    }

    private void logResult(PublishResult result) {
        log.info(
                "기동 발행 완료. 토픽={} 조회={} 성공={} 실패={}",
                result.getTopic(),
                result.getSelectedCount(),
                result.getPublishedCount(),
                result.getFailedCount());
    }
}
