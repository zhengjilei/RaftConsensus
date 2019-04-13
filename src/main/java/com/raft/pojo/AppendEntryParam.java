package com.raft.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class AppendEntryParam implements Serializable {
    private int term; // leader's term
    private String leaderId;
    private int prevLogIndex;
    private int prevLogTerm;

    private List<LogEntry> entries; // 发送的日志项
    private int leaderCommitIndex;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<LogEntry> entries) {
        this.entries = entries;
    }

    public int getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public void setLeaderCommitIndex(int leaderCommitIndex) {
        this.leaderCommitIndex = leaderCommitIndex;
    }
}
