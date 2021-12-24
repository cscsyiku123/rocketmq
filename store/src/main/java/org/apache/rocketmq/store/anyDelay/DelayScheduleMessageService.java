/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.anyDelay;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.schedule.DelayOffsetSerializeWrapper;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DelayScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final int anyDelayQueueId;
    private Timer timer;
    private MessageStore writeMessageStore;
    private long offset = 0;
    private AnyDelayConsumeQueue anyDelayConsumeQueue;
    private Long timestamp = 0l;
    private AnyDelayMessageStore anyDelayMessageStore;
    private AnyDelayDeliveryService anyDelayDeliveryService;

    public DelayScheduleMessageService(final AnyDelayMessageStore anyDelayMessageStore) {
        this.anyDelayMessageStore = anyDelayMessageStore;
        this.defaultMessageStore = anyDelayMessageStore.getDefaultMessageStore();
        this.writeMessageStore = this.defaultMessageStore;
        this.anyDelayQueueId = defaultMessageStore.getMessageStoreConfig().getAnyDelayQueueId();
        this.anyDelayConsumeQueue = anyDelayMessageStore.getAnyDelayConsumeQueue();
        this.anyDelayDeliveryService = new AnyDelayDeliveryService();
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public long computeValidTimestamp(final long timeStamp) {

        long minGreatThanTimestamp = anyDelayConsumeQueue.getMinGreatThanTimestamp(timeStamp);
        long maxLessThanTimestamp = anyDelayConsumeQueue.getMaxLessThanTimestamp(timeStamp);
        long equalsTimestamp = anyDelayConsumeQueue.getEqualsTimestamp(timeStamp);
        long minTimestamp = anyDelayConsumeQueue.getMinTimestamp();
        long maxTimestamp = anyDelayConsumeQueue.getMaxTimestamp();
        if (equalsTimestamp != 0) {
            return equalsTimestamp;
        }
        if (timeStamp < minTimestamp) {
            return minGreatThanTimestamp;
        }
        if (timeStamp > maxTimestamp) {
            return timeStamp;
        }
        return maxLessThanTimestamp;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            super.load();
            this.timer = new Timer("DelayScheduleMessageTimerThread", true);
            this.timer.schedule(new DeliverDelayedMessageTimerTask(anyDelayQueueId, offset, timestamp), FIRST_DELAY_TIME);
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            DelayScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
            anyDelayDeliveryService.start();
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
            this.anyDelayDeliveryService.stop();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxAnyDelayLevel() {
        return anyDelayQueueId;
    }

    public String encode() {
        return this.encode(false);
    }

    public boolean load() {
        boolean result = super.load();
        timestamp = offsetTable.getOrDefault(getMaxAnyDelayLevel(), 0l);
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getAnyDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                                                                                        .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    private void updateOffset(int delayLevel, long timestamp) {
        this.offsetTable.put(delayLevel, timestamp);
    }

    private long getTimeDiff(long start, long end) {
        if (end <= start) {
            return 0;
        }
        return (end - start) / 1000;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private long offset;
        private long timestamp;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset, long timestamp) {
            this.delayLevel = delayLevel;
            this.offset = offset;
            this.timestamp = timestamp;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("DelayScheduleMessageService, executeOnTimeup exception", e);
                DelayScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                        this.delayLevel, this.offset, this.timestamp), DELAY_FOR_A_PERIOD);
            }
        }

        public void executeOnTimeup() {
            AnyDelayConsumeQueue cq = anyDelayConsumeQueue;
            if (cq != null) {
                this.timestamp = DelayScheduleMessageService.this.computeValidTimestamp(timestamp);
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.timestamp, this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {

                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            log.debug("executeOnTimeup task ,timestamp=[{}],offset=[{}],size=[{}]",timestamp,offsetPy
                                    ,sizePy);
                            //投递时间
                            long tagsCode = bufferCQ.getByteBuffer().getLong();
                            long deliverTimestamp = tagsCode;
                            long now = System.currentTimeMillis();
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                            long timeDiff = DelayScheduleMessageService.this.getTimeDiff(now, deliverTimestamp);
                            //提交任务
                            addDeliveryTask(offsetPy, sizePy, timeDiff);
                        }
                        //开始下一次循环
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        DelayScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset, this.timestamp), DELAY_FOR_A_WHILE);
                        DelayScheduleMessageService.this.updateOffset(this.delayLevel, this.timestamp);
                    } finally {
                        bufferCQ.release();
                    }
                } else {
                    long maxProccessStoreTimestamp = DelayScheduleMessageService.this.anyDelayConsumeQueue.getMaxProccessStoreTimestamp();
                    //推进到下一个半小时。
                    if (maxProccessStoreTimestamp == -1 && System.currentTimeMillis() > this.timestamp + 30 * 60 * 1000 || maxProccessStoreTimestamp >= this.timestamp + 30 * 60 * 1000) {
                        this.timestamp += 30 * 60 * 1000;
                        this.offset = 0l;
                        log.info("DelayScheduleMessageService come to next time:[{}],timewheeel has [{}] task",
                                this.timestamp, DelayScheduleMessageService.this.anyDelayDeliveryService.getTaskCount());

                    }
                    DelayScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, this.offset, this.timestamp), DELAY_FOR_A_WHILE);
                }
            }
        }

        private void addDeliveryTask(long offsetPy, int sizePy, long timeDiff) {
            MessageExt msgExt =
                    DelayScheduleMessageService.this.anyDelayMessageStore.lookMessageByOffset(timestamp, offsetPy, sizePy);
            if (msgExt != null) {
                DelayScheduleMessageService.this.anyDelayDeliveryService.addTimeTask(task -> {
                    try {
                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                        log.debug("executeOnTimeup one message [{}]", msgInner);
                        PutMessageResult putMessageResult = DelayScheduleMessageService.this.writeMessageStore
                                .putMessage(msgInner);
                        if (putMessageResult != null
                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                            log.debug("executeOnTimeup put one message success [{}],put result is [{}]", msgInner,
                                    putMessageResult);
                            if (DelayScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                                                                                    .isEnableScheduleMessageStats()) {
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, putMessageResult
                                                                                            .getAppendMessageResult()
                                                                                            .getMsgNum());
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, putMessageResult
                                                                                            .getAppendMessageResult()
                                                                                            .getWroteBytes());
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, putMessageResult
                                                                                            .getAppendMessageResult()
                                                                                            .getMsgNum());
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, putMessageResult
                                                                                            .getAppendMessageResult()
                                                                                            .getWroteBytes());
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incTopicPutNums(msgInner
                                                                                            .getTopic(), putMessageResult
                                                                                            .getAppendMessageResult()
                                                                                            .getMsgNum(), 1);
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incTopicPutSize(msgInner
                                                                                                    .getTopic(),
                                                                                            putMessageResult
                                                                                                    .getAppendMessageResult()
                                                                                                    .getWroteBytes());
                                DelayScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager()
                                                                                    .incBrokerPutNums(putMessageResult
                                                                                            .getAppendMessageResult()
                                                                                            .getMsgNum());
                            }
                        }
                    } catch (Exception e) {
                        /*
                         * XXX: warn and notify me
                         */
                        log.error(
                                "DelayScheduleMessageService, messageTimeup execute error, drop it. msgExt={}, offsetPy={}, sizePy={}", msgExt, offsetPy, sizePy, e);
                    }
                }, timeDiff);
            }
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());
            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);
            return msgInner;
        }
    }
}
