package com.netease.backend.nkv.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.packets.AbstractRequestPacket;

public class DumpAllRequest extends AbstractRequestPacket {

	protected short namespace;
	protected int offset;
	protected int limit;
	
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeByte((byte) 0); // 1
		buffer.writeShort(namespace); // 2
		buffer.writeInt(offset);
		buffer.writeInt(limit);
	}
	
	@Override
	public short getNamespace() {
		return namespace;
	}
	
	@Override
	public int size() {
		return 19;
	}

	public static DumpAllRequest build(short ns, int offset, int limit) {
		DumpAllRequest req = new DumpAllRequest();
		req.namespace = ns;
		req.offset = offset;
		req.limit = limit;
		return req;
	}
}
