package org.apache.rocketmq.store.anyDelay;

import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class CommitLogDispatcherBuildConsumeQueuePre implements CommitLogDispatcher {
    private MessageStoreConfig messageStoreConfig;
    private DefaultMessageStore defaultMessageStore;
    private AnyDelayMessageStore anyDelayMessageStore;

    public CommitLogDispatcherBuildConsumeQueuePre(AnyDelayMessageStore anyDelayMessageStore) {
        this.anyDelayMessageStore = anyDelayMessageStore;
        this.defaultMessageStore = anyDelayMessageStore.getDefaultMessageStore();
        this.messageStoreConfig = anyDelayMessageStore.getMessageStoreConfig();
    }

    @Override
    public void dispatch(DispatchRequest request) {
        //不是任意延迟消息不处理
        int queueId = request.getQueueId();
        if (queueId != messageStoreConfig.getAnyDelayQueueId()) {
            return;
        }
        long commitLogOffset = request.getCommitLogOffset();
        int msgSize = request.getMsgSize();
        long storeTimestamp = request.getStoreTimestamp();
        SelectMappedBufferResult selectMappedBufferResult = this.defaultMessageStore.selectOneMessageByOffset(commitLogOffset, msgSize);
        MappedFile mappedFile =
                this.anyDelayMessageStore.getAnyDelayCommitLog().getAnyDelaymappedFileQueue().getMappedFileByTimeStamp(storeTimestamp, true);
        int wrotePosition = mappedFile.getWrotePosition();
        //负责写入queueid=100的commitlog里边
        mappedFile.appendMessage(selectMappedBufferResult.getByteBuffer());
        //修改偏移量
        request.setCommitLogOffset(wrotePosition);

        this.anyDelayMessageStore.getAnyDelayConsumeQueue().putMessagePositionInfoWrapper(request);
    }
}
