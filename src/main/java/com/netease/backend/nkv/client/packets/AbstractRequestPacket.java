package com.netease.backend.nkv.client.packets;
import org.jboss.netty.buffer.ChannelBuffer;


public abstract class AbstractRequestPacket extends AbstractPacket {
	
    public int decodeConfigVersionFrom(ChannelBuffer bb) {
		return 0;
	}

	public boolean hasConfigVersion() {
    	return false;
	}

	public abstract short getNamespace();
}
