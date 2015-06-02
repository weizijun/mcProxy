package com.netease.backend.nkv.client.rpc.protocol.tair2_3;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacketFactory;
public class PacketFactory implements NkvRpcPacketFactory {
	private static PacketFactory instance = new PacketFactory();
	private PacketFactory() { }
	
	public static PacketFactory getInstance() {
		return instance;
	}

	public NkvRpcPacket buildWithHeader(ChannelBuffer in) throws NkvRpcError {
		NkvRpcPacket rpcPacket =  PacketWrapper.buildWithHeader(in);
		return rpcPacket;
	}

	public NkvRpcPacket buildWithBody(int chid, Object body) {
		return PacketWrapper.buildWithBody(chid, (Packet)body);
	}
}

