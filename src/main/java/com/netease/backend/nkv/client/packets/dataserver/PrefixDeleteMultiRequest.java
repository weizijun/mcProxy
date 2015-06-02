package com.netease.backend.nkv.client.packets.dataserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.util.NkvConstant;

public class PrefixDeleteMultiRequest extends AbstractRequestPacket {
	protected short namespace;
	//1) pkey != null && keys != null prefixDeleteMulti
	protected byte[] pkey = null;
	protected List<byte[]> skeys = null;

	public PrefixDeleteMultiRequest(short ns, byte[] pkey, List<byte[]> skeys) {
		this.namespace = ns;
		this.pkey = pkey;
		this.skeys = skeys;
	}

	public short getNamespace() {
		return this.namespace;
	}
	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte((byte)0); // 1
		out.writeShort(namespace); // 2
		out.writeInt(skeys.size()); // 4
		for (byte[] skey : skeys) {
			int keySize = pkey.length + PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
			encodeDataMeta(out); // 36
			out.writeInt(keySize); // 4
			out.writeBytes(PREFIX_KEY_TYPE);
			out.writeBytes(pkey);
			out.writeBytes(skey);
		}
	}

	public int size() {
		int s = 7;
		for (byte[] skey : skeys) {
			s += (4 + 36 + pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		return s;
	}

	public static PrefixDeleteMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys) {
		if (ns <0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (skeys == null || skeys.size() == 0) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : skeys) {
			if (key == null || (pkey.length + key.length + PREFIX_KEY_TYPE.length) > NkvConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
			}
		}
		PrefixDeleteMultiRequest request = new PrefixDeleteMultiRequest(ns, pkey, skeys);
		return request;
	}
}
