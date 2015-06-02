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


public class PrefixDeleteMultiCast implements NkvResultCast<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(BatchReturnResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof Pair<?, ?>)) {
			throw new  NkvCastIllegalContext("context of PrefixDeleteMultiCast.");
		}

		Pair<byte[], List<byte[]>> pair = (Pair<byte[], List<byte[]>>) context;
		byte[] pkey = pair.first();
		List<byte[]> keys = pair.second();
		
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>> ();
		Set<byte[]> keySet = new TreeSet<byte[]> (NkvUtil.BYTES_COMPARATOR);
		keySet.addAll(keys);
		ResultCode code = ResultCode.castResultCode(s.getCode());
		Map<byte[], Integer>  kcmap = s.getKeyCodeMap();
		if (kcmap != null) {
			for (Map.Entry<byte[], Integer> e : kcmap.entrySet()) {
				keySet.remove(e.getKey());
				Result<Void> r = new Result<Void>();
				r.setCode(ResultCode.castResultCode(e.getValue()));
				resMap.put(e.getKey(), r);
			}
		}
		for (byte[] key : keySet) {
			Result<Void> r = new Result<Void>();
			r.setCode(ResultCode.OK);
			resMap.put(key, r);
		}
		 
		result.setCode(code);
		resMap.setCode(code);
		resMap.setKey(pkey);
		result.setResult(resMap);
		return result;
	}

}
