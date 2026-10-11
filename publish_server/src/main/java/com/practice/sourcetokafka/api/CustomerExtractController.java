package com.practice.sourcetokafka.api;

import com.practice.sourcetokafka.publish.CustomerPublishService;
import com.practice.sourcetokafka.publish.PublishResult;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 원천 데이터 발행을 수동으로 다시 실행하는 진입점.
 *
 * <p>기동 시 발행은 {@link com.practice.sourcetokafka.bootstrap.CustomerExtractStartupRunner}가 맡고,
 * 이 컨트롤러는 같은 {@link CustomerPublishService#publishAll()}을 HTTP로 한 번 더 호출한다.
 * Kafka를 앱보다 나중에 띄운 경우 이 요청으로 다시 보낸다.
 */
@RestController
@RequestMapping("/api/extract")
@RequiredArgsConstructor
public class CustomerExtractController {

    private final CustomerPublishService customerPublishService;

    /**
     * {@code POST /api/extract/customers}로 테이블 전 건을 다시 발행한다.
     *
     * <p>본문은 없다. 응답은 조회 건수, 발행 성공 건수, 실패 키 목록이다.
     * 일부 행만 실패해도 HTTP 200이며, 실패 여부는 {@code failedCount}로 구분한다.
     * DB 조회가 실패하면 이 메서드 밖으로 예외가 나가 Spring의 기본 오류 응답이 된다.
     */
    @PostMapping("/customers")
    public PublishResult extractCustomers() {
        // 기동 러너와 같은 전량 발행이다. 요청 본문은 없고, 반환 객체가 그대로 HTTP JSON이 된다.
        // 일부 행만 Kafka에 실패해도 예외가 아니므로 200과 함께 failedCount가 나간다.
        return customerPublishService.publishAll();
    }
}
