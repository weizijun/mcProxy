package com.netease.backend.nkv.client.packets.dataserver;


import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.util.NkvConstant;

/*public class BoundedIncDecRequest extends IncDecRequest {
    public BoundedIncDecRequest(short ns, byte[] pkey, byte[] skey, int count, int initValue, int expireTime, int lowBound, int upperBound) {
		super(ns, pkey, skey, count, initValue, expireTime);
		this.lowBound = lowBound;
		this.upperBound = upperBound;
	}

	protected int lowBound = Integer.MIN_VALUE;
    protected int upperBound = Integer.MAX_VALUE;
	
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		super.encodeTo(buffer);
		buffer.writeInt(lowBound);
		buffer.writeInt(upperBound);
	}
	@Override
	public int size() {
		return super.size() + 4 + 4;
	}
	
	public static BoundedIncDecRequest build(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, NkvOption opt) throws IllegalArgumentException {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && (pkey.length + skey.length + PREFIX_KEY_TYPE.length > NkvConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (lowBound > upperBound) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		BoundedIncDecRequest request = new BoundedIncDecRequest(ns, pkey, skey, value, initValue, opt.getExpire(), lowBound, upperBound);
		return request;
	}		
}*/
