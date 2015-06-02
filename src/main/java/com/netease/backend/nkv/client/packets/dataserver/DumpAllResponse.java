package com.netease.backend.nkv.client.packets.dataserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.impl.BaseEntry;
import com.netease.backend.nkv.client.packets.AbstractResponsePacket;

public class DumpAllResponse extends AbstractResponsePacket {

	private int offset = 0;
	private List<Entry<byte[], byte[]>> valueList = new ArrayList<Entry<byte[], byte[]>>();
	
	@Override
	public void decodeFrom(ChannelBuffer buffer) {
		byte[] okey = null;
		byte[] ovalue = null;
    	resultCode = buffer.readInt();
    	offset = buffer.readInt();
        int count = buffer.readInt();
        valueList = new ArrayList<Entry<byte[], byte[]>>(count);
        for (int i = 0; i < count; i++) {
        	okey = null;
        	ovalue = null;
        	
        	int size = buffer.readInt();
        	size = size & 0x3FFFFF;
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
    			okey = new byte[size];
        		buffer.readBytes(okey);
			}
				
        	int valueSize = buffer.readInt();
        	if (valueSize > 0) {
				ovalue = new byte[valueSize];
				buffer.readBytes(ovalue);
        	}
        	
    		valueList.add(new BaseEntry<byte[], byte[]>(okey, ovalue));
        }
	}
	
	public int getOffset() {
		return offset;
	}
	
	public List<Entry<byte[], byte[]>> getValues() {
		return valueList;
	}

	@Override
	public boolean hasConfigVersion() {
		return true;
	}
	
}
