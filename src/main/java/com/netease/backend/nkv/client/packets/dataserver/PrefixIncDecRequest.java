package com.netease.backend.nkv.client.packets.dataserver;

import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient.Counter;
import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.util.NkvConstant;
import com.netease.backend.nkv.client.util.NkvUtil;

public class PrefixIncDecRequest extends AbstractRequestPacket {
	private short namespace = 0;
	private byte[] pkey = null;
	private Map<byte[], Counter> skvs = null;
	public PrefixIncDecRequest(short ns, byte[] pkey, Map<byte[], Counter> skvs) {
		this.namespace = ns;
		this.pkey = pkey;
		this.skvs = skvs;
	}
	public short getNamespace() {
		return this.namespace;
	}
	public static PrefixIncDecRequest build(short ns, byte[] pkey, Map<byte[], Counter> skv) {
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
		PrefixIncDecRequest request = new PrefixIncDecRequest(ns, pkey, skv);
		return request;
	}
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		int keySize = 0;
		
		buffer.writeByte(0);
		buffer.writeShort(namespace);
		
		encodeDataMeta(buffer);
		buffer.writeInt(pkey.length + PREFIX_KEY_TYPE.length);
		buffer.writeBytes(PREFIX_KEY_TYPE);
		buffer.writeBytes(pkey);
		
		buffer.writeInt(skvs.size());
		for (Map.Entry<byte[], Counter> e : skvs.entrySet()) {
			byte[] skey = e.getKey();
			Counter counter = e.getValue();
			keySize = pkey.length + PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
			encodeDataMeta(buffer);
			buffer.writeInt(keySize);
			buffer.writeBytes(PREFIX_KEY_TYPE);
			buffer.writeBytes(pkey);
			buffer.writeBytes(skey);
			
			counter.setExpire(NkvUtil.getDuration(counter.getExpire()));
			buffer.writeInt(counter.getValue());
			buffer.writeInt(counter.getInitValue());
			buffer.writeInt(counter.getExpire());
		}
	}

	public int size() {
		int s = 1 + 2 + 4 + 40 + (pkey.length + 2) + 4;
		for (Map.Entry<byte[], Counter> e : skvs.entrySet()) {
			s += (40 + 4 + e.getKey().length + pkey.length + 12 + PREFIX_KEY_TYPE.length);
		}
		return s;
	}
}
