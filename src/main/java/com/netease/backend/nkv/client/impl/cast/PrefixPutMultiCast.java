package com.netease.backend.nkv.client.impl.cast;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.NkvClient.Pair;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.BatchReturnResponse;
import com.netease.backend.nkv.client.util.NkvUtil;

public class PrefixPutMultiCast implements NkvResultCast<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(BatchReturnResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof Pair<?,?>)) {
			throw new  NkvCastIllegalContext("context of PrefixPutMultiCast.");
		}

		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>>();

		Map<byte[], Integer> kcmap = s.getKeyCodeMap();
		@SuppressWarnings("unchecked")
		Pair<byte[], List<byte[]>> pair = (Pair<byte[], List<byte[]>>) context;
		byte[] pkey = pair.first();
		List<byte[]> keys = pair.second();
		Set<byte[]> keySet = new TreeSet<byte[]>(NkvUtil.BYTES_COMPARATOR);
		keySet.addAll(keys);
		if (kcmap != null) {
			for (Map.Entry<byte[], Integer> e : kcmap.entrySet()) {
				byte[] key = e.getKey();
				Result<Void> r = new Result<Void>();
				r.setKey(key);
				r.setCode(ResultCode.castResultCode(e.getValue()));
				resMap.put(key, r);
				keySet.remove(key);
			}
		}
		for (byte[] key : keySet) {
			Result<Void> r = new Result<Void>();
			r.setKey(key);
			r.setCode(ResultCode.OK);
			resMap.put(key, r);
		}
		resMap.setKey(pkey);
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		resMap.setCode(code);
		result.setResult(resMap);
		return result;
	}

}
