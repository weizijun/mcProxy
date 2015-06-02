package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvRpcError;
//定义网络通信协议包接口
public interface NkvRpcPacket {
	public ChannelBuffer encode() throws NkvRpcError;
	public int getChannelSeq();
	public Object getBody();
	public int getBodyLength();
	public void decodeBody() throws NkvException;
	
	public boolean hasConfigVersion();
	
	public int decodeConfigVersion() throws NkvException;
	public int decodeResultCode() throws NkvException;
	
	public boolean assignBodyBuffer(ChannelBuffer in) throws NkvRpcError;
}
