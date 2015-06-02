package com.netease.backend.nkv.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.util.NkvConstant;

public class TrafficCheckRequest extends AbstractRequestPacket {
	
	protected int ns;
	public TrafficCheckRequest(short ns) {
		this.ns = ns;
	}
	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeInt(ns);
	}
	@Override 
	public int size() {
		return 4;
	}
	static public TrafficCheckRequest build(short ns) {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		TrafficCheckRequest request = new TrafficCheckRequest(ns);
		return request;
	}
	@Override
	public short getNamespace() {
		return (short) ns;
	}
}
