package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;

import java.time.Duration;
import java.util.*;

/**
 * @author wenziyue
 */

@Slf4j@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RedisStreamUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String STREAM_KEY = "test:article:like:stream";
    private static final String GROUP_NAME = "like_group";
    private static final String CONSUMER_NAME = "consumer_1";

    private static String messageId;

    @BeforeAll
    static void clearStreamBefore(@Autowired RedisUtils redisUtils) {
        // 删除整个 stream
        redisUtils.delete(STREAM_KEY);
    }

    @Test
    @Order(1)
    @DisplayName("创建消费者组")
    void testGroupCreate() {
        redisUtils.xGroupCreate(STREAM_KEY, GROUP_NAME);
        log.info("消费者组创建成功");
    }

    @Test
    @Order(2)
    @DisplayName("添加消息到 Stream")
    void testXAdd() {
        Map<String, String> data = new HashMap<>();
        data.put("userId", "1001");
        data.put("articleId", "2001");

        messageId = redisUtils.xAdd(STREAM_KEY, data);
        log.info("添加消息成功，messageId = {}", messageId);
    }

    @Test
    @Order(3)
    @DisplayName("消费者读取消息")
    void testXReadGroup() {
        List<MapRecord<String, String, String>> records = redisUtils.xReadGroup(
                STREAM_KEY, GROUP_NAME, CONSUMER_NAME, 10, Duration.ofSeconds(2)
        );

        for (MapRecord<String, String, String> record : records) {
            log.info("读取到消息: id = {}, body = {}", record.getId(), record.getValue());
        }

        Assertions.assertFalse(records.isEmpty(), "未读取到任何消息");
        messageId = records.get(0).getId().getValue();
    }

    @Test
    @Order(4)
    @DisplayName("首次查看待确认消息摘要")
    void testXPendingSummaryFirst() {
        PendingMessagesSummary summary = redisUtils.xPendingSummary(STREAM_KEY, GROUP_NAME);

        Range<String> idRange = summary.getIdRange();

        String minId = idRange.getLowerBound().isBounded() ? idRange.getLowerBound().getValue().get() : "N/A";
        String maxId = idRange.getUpperBound().isBounded() ? idRange.getUpperBound().getValue().get() : "N/A";

        log.info("First pending summary: total={}, minId={}, maxId={}, consumers={}",
                summary.getTotalPendingMessages(),
                minId,
                maxId,
                summary.getPendingMessagesPerConsumer()
        );
    }

    @Test
    @Order(5)
    @DisplayName("确认消息已消费")
    void testXAck() {
        redisUtils.xAck(STREAM_KEY, GROUP_NAME, Collections.singletonList(messageId));
        log.info("确认消息完成，messageId = {}", messageId);
    }

    @Test
    @Order(6)
    @DisplayName("查看待确认消息摘要")
    void testXPendingSummary() {
        PendingMessagesSummary summary = redisUtils.xPendingSummary(STREAM_KEY, GROUP_NAME);

        Range<String> idRange = summary.getIdRange();

        String minId = idRange.getLowerBound().isBounded() ? idRange.getLowerBound().getValue().get() : "N/A";
        String maxId = idRange.getUpperBound().isBounded() ? idRange.getUpperBound().getValue().get() : "N/A";

        log.info("Second pending summary: total={}, minId={}, maxId={}, consumers={}",
                summary.getTotalPendingMessages(),
                minId,
                maxId,
                summary.getPendingMessagesPerConsumer()
        );
    }

    @Test
    @Order(7)
    @DisplayName("获取消费者的待确认消息列表")
    void testXPending() {
        PendingMessages pending = redisUtils.xPending(STREAM_KEY, GROUP_NAME, CONSUMER_NAME, 10);
        for (PendingMessage msg : pending) {
            log.info("未确认消息：id = {}, consumer = {}, count = {}", msg.getId(), msg.getConsumerName(), msg.getTotalDeliveryCount());
        }
    }

    @Test
    @Order(8)
    @DisplayName("从指定 ID 开始读取")
    void testXReadFromId() {
        List<MapRecord<String, String, String>> records = redisUtils.xReadFromId(STREAM_KEY, "0", 5);
        for (MapRecord<String, String, String> record : records) {
            log.info("全量读取：id = {}, value = {}", record.getId(), record.getValue());
        }
    }

    @Test
    @Order(9)
    @DisplayName("删除消息")
    void testXDel() {
        long deleted = redisUtils.xDel(STREAM_KEY, Collections.singletonList(messageId));
        log.info("删除了 " + deleted + " 条消息");
    }

    @Test
    @Order(10)
    @DisplayName("测试 xClaim 转移消息所有权")
    void testXClaim() {
        // 重新发送一条消息
        Map<String, String> data = new HashMap<>();
        data.put("userId", "1002");
        data.put("articleId", "2002");
        String newMessageId = redisUtils.xAdd(STREAM_KEY, data);

        // 模拟消费但不ack，使其进入pending
        List<MapRecord<String, String, String>> records = redisUtils.xReadGroup(
                STREAM_KEY, GROUP_NAME, CONSUMER_NAME, 1, Duration.ofSeconds(1)
        );
        Assertions.assertFalse(records.isEmpty(), "准备 claim 的消息读取失败");

        // 暂停 2 秒，使其超过 minIdleTime
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 执行 claim，将消息转移给新消费者
        String newConsumer = "consumer_claim";
        List<String> ids = Collections.singletonList(newMessageId);
        List<MapRecord<String, Object, Object>> claimed = redisUtils.xClaim(
                STREAM_KEY,
                GROUP_NAME,
                newConsumer,
                Duration.ofSeconds(1),
                ids
        );

        Assertions.assertFalse(claimed.isEmpty(), "claim 后未返回消息");
        log.info("claim 成功，消息 id = {}, 新消费者 = {}", claimed.get(0).getId(), newConsumer);
    }

    @AfterAll
    static void clearStreamAfter(@Autowired RedisUtils redisUtils) {
        // 删除整个 stream
        redisUtils.delete(STREAM_KEY);
    }
}
