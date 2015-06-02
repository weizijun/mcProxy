package com.netease.backend.nkv.client.packets.dataserver;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.packets.AbstractResponsePacket;


public class DumpKeyResponse extends AbstractResponsePacket {
	
	private int offset;
	protected List<byte[]> valueList = new ArrayList<byte[]>();
	
	@Override
	public void decodeFrom(ChannelBuffer buffer) {
		byte[] value = null;
    	resultCode = buffer.readInt();
    	offset = buffer.readInt();
        int count = buffer.readInt();
        for (int i = 0; i < count; ++i) {
        	int size = buffer.readInt();
        	size = size & 0x3FFFFF;
        	value = null;
        	if (size > 0) {
        		short prefixSize = (short)(size >> 22);
    			//with prefix key
    			if (prefixSize != 0) {
    				size -= PREFIX_KEY_TYPE.length;
    				prefixSize -= PREFIX_KEY_TYPE.length;
    				//two bytes flag
    				//buff.readShort();
    				buffer.skipBytes(PREFIX_KEY_TYPE.length);
    			}
    			value = new byte[size];
        		buffer.readBytes(value);
			}
        	valueList.add(value);
        }
    }

	public List<byte[]> getValues() {
		return valueList;
	}

	public int getResultCode() {
		return resultCode;
	}
	
	public int getOffset() {
		return offset;
	}

	@Override
	public boolean hasConfigVersion() {
		return true;
	}
}
