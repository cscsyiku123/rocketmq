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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.AllocateMappedFileService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class AnyDelayMappedFileQueue {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;
    protected final String storePath;
    protected final int mappedFileSize;

    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    private final AllocateMappedFileService allocateMappedFileService;

    private DefaultMessageStore defaultMessageStore;

    public AnyDelayMappedFileQueue(String storePath, int mappedFileSize, DefaultMessageStore defaultMessageStore,
                                   AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
        this.defaultMessageStore = defaultMessageStore;
    }

    public MappedFile getMappedFileByTimeStamp(final long timestamp) {
        Object[] mfs = this.mappedFiles.toArray();
        if (null == mfs)
            return null;
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getFileFromOffset() == getHalfTimeStamp(timestamp)) {
                return mappedFile;
            }
        }
        return null;
    }

    public long getHalfTimeStamp(final long timestamp) {
        int halfHour = 30 * 60 * 1000;
        //        long remainder = timestamp % halfHour;
        long count = timestamp / halfHour;
        return count * halfHour;
    }

    public MappedFile getMappedFileByTimeStamp(final long timestamp, boolean needCreate) {
        MappedFile mappedFileByTimeStamp = getMappedFileByTimeStamp(timestamp);
        if (mappedFileByTimeStamp == null && needCreate) {
            mappedFileByTimeStamp = tryCreateMappedFile(getHalfTimeStamp(timestamp));
        }
        return mappedFileByTimeStamp;
    }

    protected MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        return doCreateMappedFile(nextFilePath);
    }

    protected MappedFile doCreateMappedFile(String nextFilePath) {
        MappedFile mappedFile = null;
        try {
            mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
        } catch (IOException e) {
            log.error("create mappedFile exception", e);
        }
        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }
        return mappedFile;
    }

    public void setFlushedWhere(MappedFile mappedFile, long mappedFileOffset) {
        mappedFile.setFlushedPosition((int)mappedFileOffset);

    }

    public void setCommittedWhere(MappedFile mappedFile, long committedWhere) {
        mappedFile.setCommittedPosition((int)committedWhere);
    }

    public void truncateDirtyFiles(MappedFile mappedFile, long mappedFileOffset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        if (mappedFileOffset == 0) {
            mappedFile.destroy(1000);
            willRemoveFiles.add(mappedFile);
        } else {
            mappedFile.setWrotePosition((int) (mappedFileOffset % this.mappedFileSize));
            mappedFile.setCommittedPosition((int) (mappedFileOffset % this.mappedFileSize));
            mappedFile.setFlushedPosition((int) (mappedFileOffset % this.mappedFileSize));
        }
        this.deleteExpiredFile(willRemoveFiles);
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;
        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }
        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public int deleteExpiredFileByOffset(long offsetTimeStamp) {
        Object[] mfs = this.copyMappedFiles(0);
        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            int mfsLength = mfs.length - 1;
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                MappedFile mappedFile = (MappedFile) mfs[i];
                long l = mappedFile.getFileFromOffset();
                //
                if (offsetTimeStamp >= l) {
                    log.info("physic file", mappedFile.getFileName(), "delete it");
                    destroy = true;
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        deleteExpiredFile(files);
        return deleteCount;
    }

    void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }
            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        // ascending order
        files.sort(Comparator.comparing(File::getName));
        for (File file : files) {
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                         + " length not matched message store config value, ignore it");
                return true;
            }
            try {
                MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }



    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public void checkSelf() {
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        for (MappedFile mappedFile : this.mappedFiles) {
            log.debug("anydelaymappedfilequeue flush begin,file is [{}]",mappedFile.getFileName());
            mappedFile.flush(flushLeastPages);
        }
        return result;
    }


    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;
        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }
        return mappedFileFirst;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }
    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }





}
