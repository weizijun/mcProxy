package com.netease.backend.nkv.client.packets.dataserver;

import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient.Counter;
import com.netease.backend.nkv.client.util.NkvConstant;

/*public class BoundedPrefixIncDecRequest  extends PrefixIncDecRequest {
	public BoundedPrefixIncDecRequest(short ns, byte[] pkey, Map<byte[], Counter> skvs, int lowBound, int upperBound) {
		super(ns, pkey, skvs);
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
	
	public static BoundedPrefixIncDecRequest build(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound) {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (skv == null || skv.size() == 0) {
			throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
		}
		for (Map.Entry<byte[], Counter> entry : skv.entrySet()) {
			byte[] skey = entry.getKey();
			if (skey == null || (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > NkvConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
			}
			if (entry.getValue() == null) {
				throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
			}
		}
		if (lowBound > upperBound) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		BoundedPrefixIncDecRequest request = new BoundedPrefixIncDecRequest(ns, pkey, skv, lowBound, upperBound);
		return request;
	}
}*/
