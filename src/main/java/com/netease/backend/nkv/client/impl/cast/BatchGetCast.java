package com.netease.backend.nkv.client.impl.cast;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressContext;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.util.NkvUtil;


public class BatchGetCast implements NkvResultCast<GetResponse, Result<ResultMap<String, Result<byte[]>>>> {
	public Result<ResultMap<String, Result<byte[]>>> cast(GetResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof CompressContext)) {
			throw new  NkvCastIllegalContext("context of BatchGetCast.");
		}
		
		Result<ResultMap<String, Result<byte[]>>> result = new Result<ResultMap<String, Result<byte[]>>>();
		ResultMap<String, Result<byte[]>> r = new ResultMap<String, Result<byte[]>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		
		CompressContext cc = (CompressContext) context;
		@SuppressWarnings("unchecked")
		List<byte[]> keys = (List<byte[]>) (cc.getContext());
		Set<byte[]> keySet = new TreeSet<byte[]>(NkvUtil.BYTES_COMPARATOR);
		keySet.addAll(keys);
		if ((code.equals(ResultCode.OK) || code.equals(ResultCode.PART_OK)) && s.getEntrires() != null && s.getEntrires().size() > 0) {
			for (Result<byte[]> res : s.getEntrires()) {
				if (cc.isUseCompress()) {
					byte[] value = res.getResult();
					byte[] restore = NkvUtil.decompress(value);
					if (restore != null) {
						res.setResult(restore);
					}
				}
				res.setCode(code);
				r.put(new String(res.getKey()), res);
				keySet.remove(res.getKey());
			}
		}
		//with out value.
		for (byte[] key : keySet) {
			Result<byte[]> e = new Result<byte[]>();
			e.setKey(key);
			e.setCode(ResultCode.NOTEXISTS);
			r.put(new String(key), e);
		}
		r.setCode(code);
		result.setResult(r);
		return result;
	}
}
