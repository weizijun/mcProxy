package com.netease.backend.nkv.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.util.NkvConstant;
import com.netease.backend.nkv.client.util.NkvUtil;

public class IncDecRequest extends AbstractRequestPacket {
	protected short namespace = 0;
    protected int count = 1;
    protected int initValue = 0;
    protected int expireTime = 0;
    // only two cases:
    // pkey != null && skey == null
    // pkey == null && skey != null
    protected byte[] pkey = null;
    protected byte[] skey = null;
    //protected short prefixSize = 0;
    
    public short getNamespace() {
		return this.namespace;
	}
    public IncDecRequest(short ns, byte[] pkey, byte[] skey, int count, int initValue, int expireTime) {
    	this.namespace = ns;
    	this.pkey = pkey;
    	this.skey = skey;
    	this.count = count;
    	this.initValue = initValue;
    	this.expireTime = expireTime;
    }
	
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeByte((byte)0); // 1
		buffer.writeShort(namespace); // 2
		buffer.writeInt(count); // 4
		buffer.writeInt(initValue); //4
		buffer.writeInt(NkvUtil.getDuration(expireTime)); //4
		
		int keySize = pkey.length;
		if (skey != null) {
			keySize += PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		encodeDataMeta(buffer);
		buffer.writeInt(keySize);
		if (skey != null) {
			buffer.writeBytes(PREFIX_KEY_TYPE);
		}
		buffer.writeBytes(pkey);
		if (skey != null) {
			buffer.writeBytes(skey);
		}
	}
	public int size() {
		int s = 1 + 2 + 4 + 4 + 4 + 40 + pkey.length;
		return skey != null ? (s + skey.length + PREFIX_KEY_TYPE.length) : s;
	}
	
	public static IncDecRequest build(short ns, byte[] pkey, byte[] skey, int value, int initValue, NkvOption opt) throws IllegalArgumentException {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length)> NkvConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		IncDecRequest request = new IncDecRequest(ns, pkey, skey, value, initValue, opt.getExpire());
		return request;
	}	
}
