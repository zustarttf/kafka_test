package com.practice.sourcetokafka.api;

import com.practice.sourcetokafka.publish.AccountPublishService;
import com.practice.sourcetokafka.publish.CustomerPublishService;
import com.practice.sourcetokafka.publish.ProductPublishService;
import com.practice.sourcetokafka.publish.PublishResult;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 테이블별 스냅샷 발행을 수동으로 다시 실행한다. */
@RestController
@RequestMapping("/api/extract")
@RequiredArgsConstructor
public class CustomerExtractController {

    private final CustomerPublishService customerPublishService;
    private final AccountPublishService accountPublishService;
    private final ProductPublishService productPublishService;

    /** {@code POST /api/extract/customers}. 본문은 없고, 응답은 이번 발행의 건수다. */
    @PostMapping("/customers")
    public PublishResult extractCustomers() {
        return customerPublishService.publishAll();
    }

    /** {@code POST /api/extract/accounts}. */
    @PostMapping("/accounts")
    public PublishResult extractAccounts() {
        return accountPublishService.publishAll();
    }

    /** {@code POST /api/extract/products}. */
    @PostMapping("/products")
    public PublishResult extractProducts() {
        return productPublishService.publishAll();
    }
}
