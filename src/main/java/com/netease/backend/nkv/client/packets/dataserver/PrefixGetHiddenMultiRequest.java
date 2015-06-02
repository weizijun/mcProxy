package com.netease.backend.nkv.client.packets.dataserver;

import java.util.List;

import com.netease.backend.nkv.client.util.NkvConstant;
public class PrefixGetHiddenMultiRequest extends PrefixGetMultiRequest {
	public PrefixGetHiddenMultiRequest(short ns, byte[] pkey, List<byte[]> skeys) {
		super(ns, pkey, skeys);
	}

	public static PrefixGetHiddenMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys) {
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
		PrefixGetHiddenMultiRequest request = new PrefixGetHiddenMultiRequest(ns, pkey, skeys);
		return request;
	}
}
