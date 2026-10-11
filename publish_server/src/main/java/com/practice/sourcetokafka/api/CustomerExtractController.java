package com.practice.sourcetokafka.api;

import com.practice.sourcetokafka.publish.CustomerPublishService;
import com.practice.sourcetokafka.publish.PublishResult;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 원천 데이터 발행을 수동으로 다시 실행한다. */
@RestController
@RequestMapping("/api/extract")
@RequiredArgsConstructor
public class CustomerExtractController {

    private final CustomerPublishService customerPublishService;

    /** {@code POST /api/extract/customers}. 본문은 없고, 응답은 이번 발행의 건수다. */
    @PostMapping("/customers")
    public PublishResult extractCustomers() {
        return customerPublishService.publishAll();
    }
}
