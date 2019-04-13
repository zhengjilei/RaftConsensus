package com.raft.log;

import com.alibaba.fastjson.JSON;
import com.raft.pojo.LogEntry;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class LogModuleImpl implements LogModule {
    private final static byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();
    private static String dbDir;
    private static String logsDir;
    private static RocksDB rocksDB;

    static {
        dbDir = "./db/" + System.getProperty("serverPort");
        logsDir = dbDir + "/logs";
        RocksDB.loadLibrary(); // 初始化
    }

    public LogModuleImpl() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        File file = new File(logsDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            rocksDB = RocksDB.open(options, logsDir);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void write(LogEntry entry) {
        long lastIndex = getLastIndex();
        try {
            rocksDB.put(convertToBytes(lastIndex + 1), JSON.toJSONBytes(entry));
            updateLastIndex(lastIndex + 1);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public LogEntry read(long index) {
        try {
            byte[] res = rocksDB.get(convertToBytes(index));
            if (res != null)
                return (LogEntry) JSON.parseObject(res, LogEntry.class);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public LogEntry getLast() {
        long lastIndex = getLastIndex();
        try {
            if(lastIndex==-1) return null;
            byte[] result = rocksDB.get(convertToBytes(lastIndex));
            LogEntry entry = (LogEntry) JSON.parseObject(result, LogEntry.class);
            return entry;
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Long getLastIndex() {
        byte[] lastIndex = "-1".getBytes();
        try {
            lastIndex = rocksDB.get(LAST_INDEX_KEY);
            if (lastIndex == null) {
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.parseLong(new String(lastIndex));
    }

    public byte[] convertToBytes(Long a) {
        return a.toString().getBytes();
    }

    private void updateLastIndex(long index) {
        try {
            rocksDB.put(LAST_INDEX_KEY, convertToBytes(index));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }
}
