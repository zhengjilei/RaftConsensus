package com.raft;

import com.raft.pojo.ClientResp;
import com.raft.pojo.Command;
import com.raft.pojo.Request;
import com.raft.rpc.RPCClient;

/**
 * created by Ethan-Walker on 2019/4/11
 */
public class Client {
    public static void main(String[] args) {
        RPCClient rpcClient = new RPCClient();
        Request<Command> request1 = new Request<>();

        Command c1 = new Command("a", "1");
        Command c2 = new Command("b", "2");
        Command c3 = new Command("c", "3");

        request1.setReqObj(c1);
        request1.setType(Request.RequestType.CLIENT);
        request1.setUrl("localhost:8003");
        ClientResp resp = (ClientResp) rpcClient.send(request1);
        System.out.println("c1Result: =" + resp.getResult());

        request1.setReqObj(c2);
        ClientResp resp2 = (ClientResp) rpcClient.send(request1);
        System.out.println(resp2);

        request1.setReqObj(c3);
        ClientResp resp3 = (ClientResp) rpcClient.send(request1);
        System.out.println(resp3);

    }
}
