package org.apache.rocketmq.store.anyDelay;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.nio.ByteBuffer;
import java.util.List;

public class AnyDelayCommitLog {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private final AnyDelayMappedFileQueue anyDelayMappedFileQueue;
    private final MessageStoreConfig messageStoreConfig;
    private final DefaultMessageStore defaultMessageStore;

    {
        // Commitlog case files are deleted
        log.warn("The any delay commitlog files are deleted, and delete the consume queue files");
    }

    public AnyDelayCommitLog(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.messageStoreConfig = defaultMessageStore.getMessageStoreConfig();
        this.anyDelayMappedFileQueue =
                new AnyDelayMappedFileQueue(this.messageStoreConfig.getAnyDelayStorePathCommitLog(),
                        this.messageStoreConfig.getMappedFileSizeCommitLog(), defaultMessageStore,
                        defaultMessageStore.getAllocateMappedFileService());
    }

    public AnyDelayMappedFileQueue getAnyDelaymappedFileQueue() {
        return anyDelayMappedFileQueue;
    }

    public void recoverNormally() {
        boolean checkCRCOnRecover = true;
        final List<MappedFile> mappedFiles = this.anyDelayMappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            for (MappedFile mappedFile : mappedFiles) {
                log.info("recover next physics file, " + mappedFile.getFileName());
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                long mappedFileOffset = 0;
                for (; mappedFileOffset < mappedFile.getFileSize(); ) {
                    DispatchRequest dispatchRequest = this.defaultMessageStore.getCommitLog()
                                                                              .checkMessageAndReturnSize(byteBuffer,
                                                                                      checkCRCOnRecover);
                    int size = dispatchRequest.getMsgSize();
                    // Normal data
                    if (dispatchRequest.isSuccess() && size > 0) {
                        mappedFileOffset += size;
                    } else if (dispatchRequest.isSuccess() && size == 0) {
                        break;
                    }
                    // Intermediate file read error
                    else if (!dispatchRequest.isSuccess()) {
                        log.info("recover physics file end, " + mappedFile.getFileName());
                        break;
                    }
                }
                this.anyDelayMappedFileQueue.setFlushedWhere(mappedFile, mappedFileOffset);
                this.anyDelayMappedFileQueue.setCommittedWhere(mappedFile, mappedFileOffset);
                this.anyDelayMappedFileQueue.truncateDirtyFiles(mappedFile, mappedFileOffset);
            }
        }
    }

    public SelectMappedBufferResult getMessage(long timestamp, final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.anyDelayMappedFileQueue.getMappedFileByTimeStamp(timestamp, false);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public boolean load() {
        boolean result = this.anyDelayMappedFileQueue.load();
        log.info("load any delay commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void shutdown() {
        this.anyDelayMappedFileQueue.shutdown(1000);
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.anyDelayMappedFileQueue.deleteExpiredFileByOffset(offset);
        return cnt;
    }
    public boolean flush(final int flushLeastPages) {
        boolean result = this.anyDelayMappedFileQueue.flush(flushLeastPages);
        return result;
    }
}
