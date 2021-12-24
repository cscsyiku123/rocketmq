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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

public class AnyDelayConsumeQueue {
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    private final AnyDelayMappedFileQueue anyDelayMappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;
    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;

    public long getMaxProccessStoreTimestamp() {
        return maxProccessStoreTimestamp;
    }

    private long maxProccessStoreTimestamp=-1;


    public AnyDelayConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;
        this.topic = topic;
        this.queueId = queueId;
        String queueDir = this.storePath
                          + File.separator + topic
                          + File.separator + queueId;
        this.anyDelayMappedFileQueue = new AnyDelayMappedFileQueue(queueDir,mappedFileSize,defaultMessageStore,  null);
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    }

    public boolean load() {
        boolean result = this.anyDelayMappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }

    public void recover() {
        final List<MappedFile> mappedFiles = this.anyDelayMappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = 0;
            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //现在是一个时间戳
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                 + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                this.anyDelayMappedFileQueue.setFlushedWhere(mappedFile, mappedFileOffset);
                this.anyDelayMappedFileQueue.setCommittedWhere(mappedFile, mappedFileOffset);
                this.anyDelayMappedFileQueue.truncateDirtyFiles(mappedFile, mappedFileOffset);
                index++;
                if (index >= mappedFiles.size()) {
                    log.info("recover last consume queue file over, last mapped file "
                             + mappedFile.getFileName());
                    break;
                } else {
                    mappedFile = mappedFiles.get(index);
                    byteBuffer = mappedFile.sliceByteBuffer();
                    processOffset = mappedFile.getFileFromOffset();
                    mappedFileOffset = 0;
                    log.info("recover next consume queue file, " + mappedFile.getFileName());
                }
            }
        }
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.anyDelayMappedFileQueue.flush(flushLeastPages);
        return result;
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.anyDelayMappedFileQueue.deleteExpiredFileByOffset(offset);
        return cnt;
    }

    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            long storeTimestamp = request.getStoreTimestamp();
            this.maxProccessStoreTimestamp=Math.max(this.maxProccessStoreTimestamp,storeTimestamp);
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                    request.getMsgSize(), tagsCode);
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimestamp);
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                         + " failed, retry " + i + " times");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode) {
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);
        MappedFile mappedFile = this.anyDelayMappedFileQueue.getMappedFileByTimeStamp(tagsCode, true);
        if (mappedFile != null) {
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }


    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void destroy() {
        this.anyDelayMappedFileQueue.destroy();
    }

    public void checkSelf() {
        anyDelayMappedFileQueue.checkSelf();
    }


    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    public long getMinLogicOffset() {
        return 0;
    }

    public long getMinGreatThanTimestamp(long timestamp) {
        for (MappedFile mappedFile : this.anyDelayMappedFileQueue.getMappedFiles()) {
            long fileTimestamp = mappedFile.getFileFromOffset();
            if (fileTimestamp > timestamp) {
                return fileTimestamp;
            }
        }
        return 0;
    }

    public long getMaxLessThanTimestamp(long timestamp) {
        for (MappedFile mappedFile : this.anyDelayMappedFileQueue.getMappedFiles()) {
            long fileTimestamp = mappedFile.getFileFromOffset();
            if (fileTimestamp < timestamp) {
                return fileTimestamp;
            } else if (fileTimestamp > timestamp) {
                return 0;
            }
        }
        return 0;
    }
    public long getEqualsTimestamp(long timestamp) {
        for (MappedFile mappedFile : this.anyDelayMappedFileQueue.getMappedFiles()) {
            long fileTimestamp = mappedFile.getFileFromOffset();
            if (fileTimestamp == timestamp) {
                return fileTimestamp;
            }

        }
        return 0;
    }

    public long getMaxTimestamp() {
        return this.anyDelayMappedFileQueue.getMappedFiles().size() > 0 ?
                this.anyDelayMappedFileQueue.getMappedFiles().get(this.anyDelayMappedFileQueue.getMappedFiles().size()-1).getFileFromOffset() : 0l;
    }




    public long getMinTimestamp() {
        return this.anyDelayMappedFileQueue.getMappedFiles().size() > 0 ?
                this.anyDelayMappedFileQueue.getMappedFiles().get(0).getFileFromOffset() : 0l;
    }

    public SelectMappedBufferResult getIndexBuffer(final long timestamp, final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.anyDelayMappedFileQueue.getMappedFileByTimeStamp(timestamp);
            if (mappedFile != null) {
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void shutdown() {
        this.anyDelayMappedFileQueue.shutdown(1000);
    }



}
