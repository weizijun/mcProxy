package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.error.NkvRpcError;
//定义网络通信协议包的创建工厂模型，用于产生NkvRpcPacket
public interface NkvRpcPacketFactory {
    //根据channelID和request构建网络通信协议包，主要用于发包
	public NkvRpcPacket buildWithBody(int chid, Object body);
	//根据接收到的buffer数据解析构建NkvRpcPacket，主要用于收包解析过程
	public NkvRpcPacket buildWithHeader(ChannelBuffer in) throws NkvRpcError;
}
