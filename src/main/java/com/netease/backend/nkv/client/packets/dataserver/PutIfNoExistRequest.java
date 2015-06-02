package com.netease.backend.nkv.client.packets.dataserver;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressOption;
import com.netease.backend.nkv.client.util.NkvConstant;
import com.netease.backend.nkv.client.util.NkvUtil;
import com.netease.backend.nkv.client.util.NkvUtil.CompressedValue;


public class PutIfNoExistRequest extends PutRequest {

	public PutIfNoExistRequest(short ns, byte[] pkey, byte[] skey, int keyFlag,
			byte[] val, int valueFlag, short version, int expired) {
		super(ns, pkey, skey, keyFlag, val, valueFlag, version, expired);
	}
	
	public static PutIfNoExistRequest build(short ns, byte[] pkey, byte[] skey, 
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
		PutIfNoExistRequest request = new PutIfNoExistRequest(ns, pkey, skey, keyFlag, value, valueFlag, opt.getVersion(), opt.getExpire());
		if (compressOpt.isCompressEnabled() && value.length > compressOpt.getCompressThreshold()) {
			CompressedValue data = NkvUtil.compress(value, compressOpt.isUseFastCompress());
			//检查压缩过的数据是否依旧超限
			if (data.checkOverFlow(NkvConstant.MAX_VALUE_SIZE))
				throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
			request.data = data;
		}
		return request;
	}
}
