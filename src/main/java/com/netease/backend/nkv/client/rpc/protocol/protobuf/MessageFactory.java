package com.netease.backend.nkv.client.rpc.protocol.protobuf;

import org.jboss.netty.buffer.ChannelBuffer;

import com.google.protobuf.Message;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacketFactory;

public class MessageFactory implements NkvRpcPacketFactory {

	private static MessageFactory instance = new MessageFactory();
	
	private MessageManager manager = null; 

	private MessageFactory() {
		try {
			manager = new MessageManager();
		} catch (Exception e) {
			throw new RuntimeException(new NkvException(e));
		} 
	}

	public static NkvRpcPacketFactory getInstance() {
		return instance;
	}

	public NkvRpcPacket buildWithHeader(ChannelBuffer in) throws NkvRpcError {
		return MessageWrapper.buildWithHeader(in, manager);
	}

	public NkvRpcPacket buildWithBody(int chid, Object body) {
		return MessageWrapper.buildWithBody(chid, (Message) body, manager);
	}
}
