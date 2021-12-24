package org.apache.rocketmq.store.anyDelay;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AnyDelayMessageStore {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private final ScheduledExecutorService anyDelayMessageStoreScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("AnyDelayMessageStoreScheduledThread"));
    private AnyDelayCommitLog anyDelayCommitLog;
    private DefaultMessageStore defaultMessageStore;
    private MessageStoreConfig messageStoreConfig;
    private AnyDelayConsumeQueue anyDelayConsumeQueue;
    private CommitLogDispatcherBuildConsumeQueuePre commitLogDispatcherBuildConsumeQueuePre;
    private DelayScheduleMessageService delayScheduleMessageService;
    private long anyDelayfileReservedTimeSeconds;

    public AnyDelayMessageStore(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.messageStoreConfig = this.defaultMessageStore.getMessageStoreConfig();
        //consumerqueue
        this.anyDelayConsumeQueue = new AnyDelayConsumeQueue(
                TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                messageStoreConfig.getAnyDelayQueueId(),
                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.defaultMessageStore);
        //commit log
        this.anyDelayCommitLog = new AnyDelayCommitLog(this.defaultMessageStore);
        this.delayScheduleMessageService = new DelayScheduleMessageService(this);
        this.commitLogDispatcherBuildConsumeQueuePre = new CommitLogDispatcherBuildConsumeQueuePre(this);
        this.anyDelayfileReservedTimeSeconds = this.messageStoreConfig.getAnyDelayfileReservedTime() * 1000 * 60 * 60;
    }

    public boolean load() {
        boolean b1 = this.anyDelayConsumeQueue.load();
        log.info("AnyDelayMessageStore load anyDelayConsumeQueue，result=[{}]", b1);
        boolean b2 = this.anyDelayCommitLog.load();
        log.info("AnyDelayMessageStore load anyDelayCommitLog，result=[{}]", b2);
        boolean b3 = delayScheduleMessageService.load();
        log.info("AnyDelayMessageStore load delayScheduleMessageService，result=[{}]", b3);
        this.recover();
        return b1 && b2 && b3;
    }

    public void recover() {
        this.anyDelayConsumeQueue.recover();
        log.info("AnyDelayMessageStore recover anyDelayConsumeQueue，result=[{}]");
        this.anyDelayCommitLog.recoverNormally();
        log.info("AnyDelayMessageStore recover anyDelayCommitLog，result=[{}]");
    }

    public void start() {
        this.defaultMessageStore.getDispatcherList().addFirst(this.commitLogDispatcherBuildConsumeQueuePre);
        log.info("AnyDelayMessageStore start commitLogDispatcherBuildConsumeQueuePre");
        this.delayScheduleMessageService.start();
        log.info("AnyDelayMessageStore start delayScheduleMessageService");
        //定期删除
        long timestamp = System.currentTimeMillis() - this.anyDelayfileReservedTimeSeconds;
        this.anyDelayMessageStoreScheduledExecutorService.scheduleAtFixedRate(() -> {
            this.anyDelayCommitLog.deleteExpiredFile(timestamp);
            this.anyDelayConsumeQueue.deleteExpiredFile(timestamp);
            log.info("AnyDelayMessageStore start delaySchedule Delete file,period is [{}] seconds,delete file befor "
                     + "timestamp "
                     + "[{}]",this.anyDelayfileReservedTimeSeconds,timestamp);
        }, 0, 10, TimeUnit.SECONDS);
        log.info("AnyDelayMessageStore start delaySchedule Delete file");
        //定期刷新
        this.anyDelayMessageStoreScheduledExecutorService.scheduleAtFixedRate(() -> {
            this.anyDelayCommitLog.flush(0);
            this.anyDelayConsumeQueue.flush(0);
            log.info("AnyDelayMessageStore start auto flush");
        }, 0, 10, TimeUnit.SECONDS);
        log.info("AnyDelayMessageStore start auto flush");



    }

    public MessageExt lookMessageByOffset(long timestamp, long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.anyDelayCommitLog.getMessage(timestamp, commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }
        return null;
    }

    public void shutdown() {
        this.anyDelayCommitLog.shutdown();
        this.anyDelayConsumeQueue.shutdown();
        this.delayScheduleMessageService.shutdown();
        this.anyDelayMessageStoreScheduledExecutorService.shutdown();
        try {
            this.anyDelayMessageStoreScheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public AnyDelayCommitLog getAnyDelayCommitLog() {
        return anyDelayCommitLog;
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public AnyDelayConsumeQueue getAnyDelayConsumeQueue() {
        return anyDelayConsumeQueue;
    }

    public CommitLogDispatcherBuildConsumeQueuePre getCommitLogDispatcherBuildConsumeQueuePre() {
        return commitLogDispatcherBuildConsumeQueuePre;
    }

    public DelayScheduleMessageService getDelayScheduleMessageService() {
        return delayScheduleMessageService;
    }
}
