package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.error.NkvRpcError;

public interface NkvRpcPacketFactory {
	public NkvRpcPacket buildWithBody(int chid, Object body);
	public NkvRpcPacket buildWithHeader(ChannelBuffer in) throws NkvRpcError;
}
