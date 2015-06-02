package com.netease.backend.nkv.client.rpc.protocol.tair2_3;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;


public class PacketWrapper implements NkvRpcPacket {
	
	private PacketHeader header;
	private Packet body = null;
	private ChannelBuffer lazyBody;
	
	private PacketWrapper (ChannelBuffer in) throws NkvRpcError {
		header = new PacketHeader(in);
	}
	
	private PacketWrapper (int chid, Packet body) {
		this.body = body;
		header = new PacketHeader(chid, PacketManager.getPacketCode(body.getClass()));
	}
	
	public static PacketWrapper buildWithHeader(ChannelBuffer in) throws NkvRpcError {
		return  new PacketWrapper(in);
	}
	
	public static PacketWrapper buildWithBody(int chid, Packet body) {
		return new PacketWrapper(chid, body);
	}
	
	public Packet getBody() {
		return body;
	}
	
	public void setBody(Packet body) {
		this.body = body;
		header.setPacketCode(PacketManager.getPacketCode(body.getClass()));
	}
	
	public boolean hasBody() {
		return body != null;
	}
	
	public ChannelBuffer encode() {
		
		ChannelBuffer buffer = ChannelBuffers.buffer(PacketHeader.HEADER_SIZE + body.size());
		
		header.encodeTo(buffer);
		body.encodeTo(buffer);
		header.encodeLength(buffer);
		
		buffer.resetReaderIndex();
		return buffer;
	}
	
	public int getBodyLength() {
		return header.getBodyLength();
	}
	
	public int getPacketCode() {
		return header.getPacketCode();
	}
	
	public int getHeaderLength() {
		return PacketHeader.HEADER_SIZE;
	}
	
	public int getChannelSeq() {
		return header.getChannelId();
	}
	//根据response中class构建body的packet
	public boolean assignBodyBuffer(ChannelBuffer in) throws NkvRpcError {
		if (in.readableBytes() < getBodyLength()) {
			return false;
		}
		lazyBody = in.readSlice(getBodyLength());
		Class<? extends Packet> cls = PacketManager.getPacketClass(getPacketCode());
		if (cls == null) {
			throw new NkvRpcError("unknow packet " + getPacketCode());
		}
		try {
			this.body = cls.newInstance();
		} catch (Exception e) {
			throw new NkvRpcError(e);
		};
		return true;
	}
	
	public int decodeConfigVersion() throws NkvException {
		return body.decodeConfigVersionFrom(lazyBody);
	}

	public void decodeBody() throws NkvException {
		decodeBody(lazyBody);
	}
	
	private boolean decodeBody(ChannelBuffer in) {
		body.decodeFrom(in);
		return true;
	}

	public boolean hasConfigVersion() {
		return body.hasConfigVersion();
	}

	public int decodeResultCode() throws NkvException {
		return body.decodeResultCodeFrom(lazyBody);
	}
}
