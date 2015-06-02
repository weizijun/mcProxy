package com.netease.backend.nkv.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.rpc.net.FlowLimit.FlowStatus;

public class TrafficCheckResponse extends AbstractResponsePacket {
	protected int ns;
	protected FlowStatus status;
	public short getNamespace() {
		return (short) ns;
	}
	public FlowStatus getStatus() {
		return status;
	}
	public boolean hasConfigVersion() {
		return false;
	}
	@Override
	public void decodeFrom(ChannelBuffer buff) {
		int s = buff.readInt();
		switch (s) {
		case 0:
			status = FlowStatus.DOWN;
			break;
		case 1:
			status = FlowStatus.KEEP;
			break;
		case 2:
			status = FlowStatus.UP;
			break;
		default:
			status = FlowStatus.UNKNOWN;
		}
		ns = buff.readInt();
	}
	public int decodeResultCodeFrom(ChannelBuffer bb) {
		return 0;
	}
}
