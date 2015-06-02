/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.netease.backend.nkv.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressOption;
import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.util.NkvConstant;
import com.netease.backend.nkv.client.util.NkvUtil;
import com.netease.backend.nkv.client.util.NkvUtil.CompressedValue;

//单个的put操作和prefix put操作，都是走PutRequest packet流程
public class PutRequest extends AbstractRequestPacket {
	private static final int HEAD_LEN =  9 + 40 + 40;
	private static final int COM_HEAD_LEN = HEAD_LEN + 8;
	protected short namespace;
	protected short version;
	protected int expired;
	protected byte[] pkey;
	protected byte[] skey;
	protected int keyFlag = 0;
	protected byte[] val;
	protected int valueFlag = 0;
	protected CompressedValue data = null;

	public PutRequest(short ns, byte[] pkey, byte[] skey, int keyFlag, byte[] val, int valueFlag,  short version, int expired) {
		this.namespace = ns;
		this.version = version;
		this.expired = expired;
		this.pkey = pkey;
		this.skey = skey;
		this.keyFlag = keyFlag;
		this.val = val;
		this.valueFlag = valueFlag;
	}
	
	public PutRequest(short ns, byte[] pkey, byte[] skey, int keyFlag, CompressedValue data, int valueFlag,  short version, int expired) {
		this.namespace = ns;
		this.version = version;
		this.expired = expired;
		this.pkey = pkey;
		this.skey = skey;
		this.keyFlag = keyFlag;
		this.data = data;
		this.valueFlag = valueFlag;
	}
	
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeByte((byte) 0); //1
		buffer.writeShort(namespace); //2
		buffer.writeShort(version); //2 
		buffer.writeInt(NkvUtil.getDuration(expired)); //4
		//prefix
		int keySize = pkey.length;
		if (skey != null) {
			keySize += PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		//using static buffer
		encodeDataMeta(buffer, keyFlag);
		buffer.writeInt(keySize);
		if (skey != null) {
			//with prefix key
			buffer.writeBytes(PREFIX_KEY_TYPE);
		}
		buffer.writeBytes(pkey);
		if (skey != null) {
			buffer.writeBytes(skey);
		}

		encodeDataMeta(buffer, valueFlag);
		if (data == null) {
			buffer.writeInt(val.length);
			buffer.writeBytes(val);
		} else
			data.encode(buffer);
	}
	
	public int size() {
		int size = pkey.length;
		if (skey != null) {
			size += skey.length;
			size += PREFIX_KEY_TYPE.length;
		}
		if (data != null)
			size += COM_HEAD_LEN + data.getSize();
		else
			size += HEAD_LEN + val.length;
		return size;
	}

	public static PutRequest build(short ns, byte[] pkey, byte[] skey, 
			int keyFlag, byte[] value, int valueFlag, NkvOption opt, CompressOption compressOpt) {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE || (skey != null && ((skey.length + pkey.length + PREFIX_KEY_TYPE.length)> NkvConstant.MAX_KEY_SIZE))) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (value == null || !compressOpt.isCompressEnabled() && value.length > NkvConstant.MAX_VALUE_SIZE) {
			throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
		}
		//we must create the instance
		PutRequest request = new PutRequest(ns, pkey, skey, keyFlag, value, valueFlag, opt.getVersion(), opt.getExpire());
		if (compressOpt.isCompressEnabled() && value.length > compressOpt.getCompressThreshold()) {
			CompressedValue data = NkvUtil.compress(value, compressOpt.isUseFastCompress());
			//检查压缩过的数据是否依旧超限
			if (data.checkOverFlow(NkvConstant.MAX_VALUE_SIZE))
				throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
			request.data = data;
		}
		return request;
	}
	
	public short getNamespace() {
		return this.namespace;
	}
}
