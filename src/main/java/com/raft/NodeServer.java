package com.raft;

import com.raft.log.LogModule;
import com.raft.log.LogModuleImpl;
import com.raft.pojo.*;
import com.raft.rpc.RPCClient;
import com.raft.rpc.RPCServer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * created by Ethan-Walker on 2019/4/9
 */
public class NodeServer {
    private static final long ELECTION_TIMEOUT = 150; // 基数:150 毫秒
    private static final long MAX_ELECTION_TIMEOUT = 300; // 最大超时时间

    private volatile long prevElectionStamp;
    private volatile long prevHeartBeatStamp;

    private long electionTimeout = 10 * 1000; // 10s 选举过期，重新选举
    private long heartbeatTimeout = 5 * 1000;// 心跳间隔，
    // 各节点持久存在
    private volatile int currentTerm;
    private volatile String voteFor; // 为哪一个candidate 投票(IP:port)

    // 各节点上可变信息
    private int commitIndex;    // 已经提交的最大的日志索引，通过读取持久化信息-SegmentLog 得到
    private int lastApplied;    // 已经应用到状态机的最大日志索引

    private static RPCServer rpcServer;
    private static RPCClient rpcClient;
    private static ScheduledExecutorService threadPool;

    private volatile LogModule logModule;

    private PeerSet peerSet;
    private ReentrantLock lock;

    // leader 中不稳定存在
    private Map<Peer, Long> nextIndex; // nextIndex.get(id) 表示要发送给 follower=id 的下一个日志条目的索引
    private Map<Peer, Long> matchIndex; // matchIndex.get(id) 表示 follower=id  已经匹配的最大日志索引

    private volatile NodeStatus status;

    public NodeServer() {
        this.status = NodeStatus.FOLLOWER;
    }

    public Response handleRequestVote(VoteParam param) {
        System.out.println("handleRequestVote------------: selfTerm=" + currentTerm + ",voteFor=" + voteFor);
        System.out.println(param);

        lock.lock();
        prevElectionStamp = System.currentTimeMillis();// 定时器重置，防止一个 term 出现多个 candidate

        try {
            if (status == NodeStatus.LEADER || param.getTerm() <= currentTerm) { // == 也不投，== 说明当前节点成为 candidate 或者 已经为其他节点投过票
                System.out.println("拒绝投票--------------");
                return VoteResult.no(currentTerm);
            }
//        if (voteFor == null || voteFor == param.getCandidateId()) { 不要根据 voteFor 判断是否可以支持投票，而是根据任期 term> currentTerm 决定是否投票
            LogEntry lastEntry = null;
            if ((lastEntry = logModule.getLast()) != null) {
                if (lastEntry.getTerm() > param.getPrevLogTerm()) {
                    System.out.println("拒绝投票----------------");
                    return VoteResult.no(currentTerm);
                }
                if (lastEntry.getIndex() > param.getPrevLogIndex()) {
                    System.out.println("拒绝投票--------------");
                    return VoteResult.no(currentTerm);
                }
            }
            System.out.println("同意投票-------------------");
            status = NodeStatus.FOLLOWER;
            voteFor = param.getCandidateId();

            return VoteResult.yes(currentTerm);
//            }
        } finally {
            lock.unlock();
        }

    }

    public void init(List<String> ipAddrs, int selfPort, String selfAddr) {
        // 设置节点信息
        peerSet = new PeerSet();
        Peer cur = new Peer(selfAddr, selfPort);
        List<Peer> peers = new ArrayList<>();
        List<Peer> otherPeers = new ArrayList<>();


        for (String ipAddr : ipAddrs) {
            Peer p = new Peer(ipAddr);
            peers.add(p);
            if (!p.equals(cur)) {
                otherPeers.add(p);
            }
        }
        peerSet.setSelf(cur);
        peerSet.setSet(peers);
        peerSet.setOtherPeers(otherPeers);


        // 默认初始状态 follower
        status = NodeStatus.FOLLOWER;
        lock = new ReentrantLock();

        // 创建 rpc server 并启动
        rpcServer = new RPCServer(selfPort, this);
        rpcServer.start();
        rpcClient = new RPCClient();

        // 创建日志模块
        logModule = new LogModuleImpl();

        // 创建线程池，执行各项任务
        threadPool = Executors.newScheduledThreadPool(3);

        // 启动定时周期性选举任务
        threadPool.scheduleAtFixedRate(new ElectionTask(), 30000, 3000, TimeUnit.MILLISECONDS); // 延迟1s执行，每隔500 ms 检查是否需要重新选举
        // 启动定时周期性心跳任务
        threadPool.scheduleAtFixedRate(new HeartbeatTask(), 30000, heartbeatTimeout, TimeUnit.MILLISECONDS); // 延迟 3s 执行，每隔500ms 检查是否需要发送心跳
        // 获取当前任期
        LogEntry entry = logModule.getLast();
        if (entry != null) {
            currentTerm = entry.getTerm();
        }
        // entry==null, 说明任期为 0, 默认值
    }

    class ElectionTask implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                if (status == NodeStatus.LEADER)
                    return;
                System.out.println("election task");
                long current = System.currentTimeMillis();
                if (current - prevElectionStamp < electionTimeout) {
                    // 选举时间未超时，不需要重新选举
                    return;
                }
                Date date = new Date(System.currentTimeMillis());
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");
                String time = format.format(date);
                System.out.println("-------------------------开始选举-----" + time + "------------------------------");
                // 选举超时，重新选举
                // 1. 更新状态
                status = NodeStatus.CANDIDATE;
                electionTimeout = electionTimeout + (int) (Math.random() * 5000); // 超时之后，延长超时时间，但仍然是随机
                prevElectionStamp = System.currentTimeMillis(); // 设置新的选举时间
                currentTerm = currentTerm + 1;
                voteFor = peerSet.getSelf().getAddr();

                // 2. 获取其他节点信息, 向其他各个节点发送 投票请求
                List<Peer> otherPeers = peerSet.getOtherPeers();

                System.out.println("otherPeers:" + otherPeers);

                LogEntry lastEntry = logModule.getLast();

                List<Future<Response>> resp = new ArrayList<>();
                Future<Response> future = null;

                for (Peer peer : otherPeers) {

                    future = threadPool.submit(new Callable<Response>() {
                        @Override
                        public Response call() throws Exception {
                            VoteParam param = new VoteParam();
                            param.setTerm(currentTerm);
                            param.setCandidateId(peerSet.getSelf().getAddr());
                            param.setPrevLogIndex(lastEntry != null ? lastEntry.getIndex() : -1);
                            param.setPrevLogTerm(lastEntry != null ? lastEntry.getTerm() : -1);

                            Request<VoteParam> voteRequest = new Request<>();
                            voteRequest.setDesc("request-id=向" + peer.getAddr() + "发送投票");
                            voteRequest.setReqObj(param);
                            voteRequest.setType(Request.RequestType.VOTE);
                            voteRequest.setUrl(peer.getAddr());

                            System.out.println("向" + peer.getAddr() + "发送投票请求");
                            return rpcClient.send(voteRequest);
                        }
                    });
                    resp.add(future);
                }

                CountDownLatch latch = new CountDownLatch(resp.size());

                AtomicInteger countYes = new AtomicInteger(1);
                // 处理返回结果
                for (Future<Response> f : resp) {
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                VoteResult v = (VoteResult) f.get();
                                if (v.isVoteGranted()) {
                                    countYes.incrementAndGet();
                                } else {
                                    // 可能需要更新 term
                                    if (v.getTerm() > currentTerm) {
                                        currentTerm = v.getTerm();
                                    }
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                }
                try {
                    latch.await(5000, TimeUnit.MILLISECONDS);// 发送信息到返回结果超时时间 4s
                } catch (InterruptedException e) {
                    System.out.println("等待超时");// 超时不抛出，由于少部分节点一直没响应，但是投票结果已经达到多数，则仍然可以成为 leader
                }
                if (status == NodeStatus.FOLLOWER) {
                    // 在等待期间，同意其他candidate 的投票，或者收到其他已经被选为 leader 的心跳/日志项，当前节点变为 follower
                    System.out.println("等待投票结果期间，已经收到其他被确认为 leader=" + peerSet.getLeader().getAddr() + " 的心跳");
                    // 成为 follower, voteFor 为leaderID
                    return;
                }
                System.out.println("countYes=" + countYes);

                if (countYes.get() > peerSet.getSet().size() / 2) {
                    // 当前节点成为  LEADER
                    status = NodeStatus.LEADER;
                    peerSet.setLeader(peerSet.getSelf());
                    initNextMatchIndex();
                    System.out.println("当前节点成为 leader ");
                } else {
                    System.out.println("当前节点不能成为 leader,重新选举");
                }
                voteFor = null; // 1. 成为leader后，设置 voteFor=null;2.既没成为leader，也没成为follower,设为 null

            } catch (Exception e) {
                System.out.println("error happen in election");
            } finally {
                lock.unlock();
            }

        }
    }

    /**
     * 当前节点成为 leader 后，初始化 nextIndex[] , matchIndex[]
     */
    public void initNextMatchIndex() {
        nextIndex = new HashMap<>();
        matchIndex = new HashMap<>();
        List<Peer> otherPeers = peerSet.getOtherPeers();
        long lastIndex = logModule.getLastIndex();
        for (Peer p : otherPeers) {
            nextIndex.put(p, lastIndex + 1);
            matchIndex.put(p, 0L);
        }
    }

    /**
     * 接收到心跳如何处理
     *
     * @param reqObj
     * @return
     */
    public Response handleHeartbeat(AppendEntryParam reqObj) {
        lock.lock();
        try {
            if (reqObj.getTerm() < currentTerm) {
                return AppendEntryResult.no(currentTerm);
            }
            // 更新时间戳
            prevElectionStamp = System.currentTimeMillis();
            prevHeartBeatStamp = System.currentTimeMillis();
            System.out.println("接受到来自 " + reqObj.getLeaderId() + " 的心跳 term=" + reqObj.getTerm() + ", " + System.currentTimeMillis());

            // 设置主从关系
            peerSet.setLeader(new Peer(reqObj.getLeaderId()));
            if (status != NodeStatus.FOLLOWER) {
                status = NodeStatus.FOLLOWER;
            }
            if (currentTerm != reqObj.getTerm()) {
                currentTerm = reqObj.getTerm();
            }
        } finally {
            lock.unlock();
        }

        return AppendEntryResult.yes(currentTerm);

    }

    class HeartbeatTask implements Runnable {
        @Override
        public void run() {
            try {
                if (status != NodeStatus.LEADER)
                    return;
                long current = System.currentTimeMillis();
                if (current - prevHeartBeatStamp < heartbeatTimeout) { // 每隔 heartBeatTimeOut 才发送新的心跳
                    return;
                }

                // 向所有其他follower 发送心跳，心跳参数只需要设置必要的几个
                System.out.println("--------------发送心跳-------------------------------");
                AppendEntryParam param = new AppendEntryParam();
                param.setEntries(null);
                param.setLeaderId(peerSet.getSelf().getAddr());
                param.setTerm(currentTerm);

                List<Peer> otherPeers = peerSet.getOtherPeers();
                System.out.println("otherpeers:" + otherPeers);

                for (Peer peer : otherPeers) {
                    Request<AppendEntryParam> request = new Request<>();
                    request.setReqObj(param);
                    request.setType(Request.RequestType.HEARTBEAT);
                    request.setUrl(peer.getAddr());
                    request.setDesc("request-id = 向" + request.getUrl() + "发送心跳, time=" + System.currentTimeMillis());

                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            AppendEntryResult resp = (AppendEntryResult) rpcClient.send(request);
                            if (resp == null) {
                                return;
                            }
                            if (resp.getTerm() > currentTerm) {
                                status = NodeStatus.FOLLOWER;
                                currentTerm = resp.getTerm();
                                voteFor = null;
                                System.out.println(request.getDesc() + " =>心跳失败");
                            } // else 不处理
                            else {
                                System.out.println(request.getDesc() + " =>心跳成功");
                            }
                        }
                    });
                }

            } catch (Exception e) {
                System.out.println("heartbeat 出现异常");
            }
        }

    }

    public Response redirect(Request request) {
        if (peerSet.getLeader() == null)
            return ClientResp.no("no leader");
        request.setUrl(peerSet.getLeader().getAddr());
        return rpcClient.send(request);
    }

    public Response handleClientRequest(Request<Command> request) {
        if (status != NodeStatus.LEADER) {
            return redirect(request);
        }
        // 当前节点是 leader

        LogEntry entry = new LogEntry(currentTerm, commitIndex + 1, request.getReqObj());

        List<Peer> otherPeers = peerSet.getOtherPeers();
        for (Peer p : otherPeers) {
            threadPool.submit(new Callable<AppendEntryResult>() {
                @Override
                public AppendEntryResult call() throws Exception {
                    AppendEntryParam param = new AppendEntryParam();
                    param.setTerm(currentTerm);
                    param.setEntries(Arrays.asList(entry));
                    param.setLeaderId(peerSet.getLeader().getAddr());
                    param.setLeaderCommitIndex(commitIndex);

                    LogEntry lastEntry = logModule.getLast();
                    if (lastEntry != null) {
                        param.setPrevLogTerm(lastEntry.getTerm());
                        param.setPrevLogIndex(lastEntry.getIndex());
                    }
                    Request<AppendEntryParam> req = new Request<>();
                    req.setUrl(p.getAddr());
                    req.setReqObj(param);
                    req.setDesc("向" + p.getAddr() + "发送" + entry);
                    return null;
                }
            });

        }
        return null;
    }

    public Response handleAppendEntry(AppendEntryParam param) {
        return null;
    }

    public void destroy() {
        rpcServer.stop();
    }

}
