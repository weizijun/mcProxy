package com.netease.backend.nkv.client.packets.dataserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.util.NkvConstant;

public class TouchRequest extends GetRequest {
	
	private int expire;

	public TouchRequest(short namespace, byte[] pkey, byte[] skey, int expire) {
		super(namespace, pkey, skey);
		this.expire = expire;
	}
	
	public TouchRequest(short namespace, List<byte[]> keys, int expire) {
		super(namespace, keys);
		this.expire = expire;
	}

	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte((byte)0); // 1
		out.writeShort(namespace); // 2
		out.writeInt(expire);
		//single key
		if (pkey != null && keys == null) {
			out.writeInt(1); // 4
			int keySize = pkey.length ;
			if (skey != null) {
				keySize += PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
			}
			encodeDataMeta(out); // 36
			out.writeInt(keySize);  // 4
			if (skey != null) {
				out.writeBytes(PREFIX_KEY_TYPE);
			}
			out.writeBytes(pkey);
			if (skey != null) {
				out.writeBytes(skey);
			}
		}
		else if (keys != null) {
			out.writeInt(keys.size()); // 4
			for (byte[] key : keys) {
				encodeDataMeta(out); // 36
				out.writeInt(key.length);
				out.writeBytes(key);
			}
		}
		else {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
	}

	public int size() {
		return super.size() + 4;
	}
	
	public static TouchRequest build(short ns, byte[] pkey, byte[] skey, int expireTime) throws IllegalArgumentException {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length) > NkvConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		TouchRequest request = new TouchRequest(ns , pkey, skey, expireTime);
		return request;
	}
	
	public static TouchRequest build(short ns, List<byte[]> keys, NkvOption opt) throws IllegalArgumentException {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (keys == null || keys.size() == 0) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : keys) {
			if (key == null || key.length > NkvConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
			}
		}
		TouchRequest request = new TouchRequest(ns, keys, opt.getExpire());
		return request;
	}
}
