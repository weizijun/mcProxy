package com.netease.backend.nkv.client.packets.dataserver;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.NkvClient;
import com.netease.backend.nkv.client.NkvClient.Pair;
import com.netease.backend.nkv.client.NkvClient.RequestOption;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressOption;
import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.util.NkvConstant;
import com.netease.backend.nkv.client.util.NkvUtil;
import com.netease.backend.nkv.client.util.NkvUtil.CompressedValue;

public class PrefixPutMultiRequest extends AbstractRequestPacket {
	protected short namespace;
	protected byte[] pkey = null;
	protected Map<byte[], Pair<byte[], RequestOption>> kvs = null; // = new
																	// HashMap<byte[],
																	// Pair<byte[],
																	// RequestOption>>
																	// ();
	//compressed value
	protected Map<byte[], Pair<CompressedValue, RequestOption>> ckvs = null;
	
	protected Map<byte[], Pair<byte[], RequestOption>> cvs = null; // = new
																	// HashMap<byte[],
																	// Pair<byte[],
																	// RequestOption>>
																	// ();

	public PrefixPutMultiRequest(short ns, byte[] pkey,
			Map<byte[], Pair<byte[], RequestOption>> kvs,
			Map<byte[], Pair<byte[], RequestOption>> cvs) {
		this.namespace = ns;
		this.kvs = kvs;
		this.cvs = cvs;
		this.pkey = pkey;
	}

	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte((byte) 0); // 1
		out.writeShort(namespace); // 2

		encodeDataMeta(out);
		out.writeInt(pkey.length + PREFIX_KEY_TYPE.length);
		out.writeBytes(PREFIX_KEY_TYPE);
		out.writeBytes(pkey);
		int kvSize = 0;
		if (kvs != null) {
			kvSize += kvs.size();
		}
		if (ckvs != null) {
			kvSize += ckvs.size();
		}
		if (cvs != null) {
			kvSize += cvs.size();
		}
		out.writeInt(kvSize);
		if (kvs != null) {
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : kvs
					.entrySet()) {
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				if (skey == null || value == null || value.isAvaliable() == false) {
					throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
				}
				int keySize = pkey.length + PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
				

				encodeDataMeta(out, value.second().getVersion(), NkvUtil.getDuration(value.second().getExpire()));
				out.writeInt(keySize);
				out.writeBytes(PREFIX_KEY_TYPE);
				out.writeBytes(pkey);
				out.writeBytes(skey);

				encodeDataMeta(out);
				out.writeInt(value.first().length);
				out.writeBytes(value.first());
			}
		}
		if (ckvs != null) {
			for (Map.Entry<byte[], Pair<CompressedValue, RequestOption>> entry : ckvs
					.entrySet()) {
				byte[] skey = entry.getKey();
				Pair<CompressedValue, RequestOption> value = entry.getValue();
				if (skey == null || value == null || value.isAvaliable() == false) {
					throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
				}
				int keySize = pkey.length + PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
				

				encodeDataMeta(out, value.second().getVersion(), NkvUtil.getDuration(value.second().getExpire()));
				out.writeInt(keySize);
				out.writeBytes(PREFIX_KEY_TYPE);
				out.writeBytes(pkey);
				out.writeBytes(skey);

				encodeDataMeta(out);
				value.first().encode(out);
			}
		}
		if (cvs != null) {
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : cvs
					.entrySet()) {
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				if (skey == null || value == null || value.isAvaliable() == false) {
					throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
				}
				int keySize = pkey.length + PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);

				encodeDataMeta(out, value.second().getVersion(), NkvUtil.getDuration(value.second().getExpire()));
				out.writeInt(keySize);
				out.writeBytes(PREFIX_KEY_TYPE);
				out.writeBytes(pkey);
				out.writeBytes(skey);

				encodeDataMeta(out, NkvConstant.NKV_ITEM_FLAG_ADDCOUNT);
				out.writeInt(value.first().length);
				out.writeBytes(value.first());
			}
		}
	}

	public int size() {
		int s = 1 + 2 + 40 + (pkey.length + PREFIX_KEY_TYPE.length) + 4;
		if (kvs != null) {
			Iterator<Map.Entry<byte[], Pair<byte[], RequestOption>>> i = kvs
					.entrySet().iterator();
			while (i.hasNext()) {
				Map.Entry<byte[], Pair<byte[], RequestOption>> entry = i.next();
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				s += (40 + (pkey.length + skey.length + PREFIX_KEY_TYPE.length));
				s += (40 + value.first().length);
			}
		}
		if (ckvs != null) {
			Iterator<Map.Entry<byte[], Pair<CompressedValue, RequestOption>>> i = ckvs
					.entrySet().iterator();
			while (i.hasNext()) {
				Map.Entry<byte[], Pair<CompressedValue, RequestOption>> entry = i.next();
				byte[] skey = entry.getKey();
				Pair<CompressedValue, RequestOption> value = entry.getValue();
				s += (40 + (pkey.length + skey.length + PREFIX_KEY_TYPE.length));
				s += (40 + value.first().getSize());
			}
		}
		if (cvs != null) {
			Iterator<Map.Entry<byte[], Pair<byte[], RequestOption>>> i = cvs
					.entrySet().iterator();
			while (i.hasNext()) {
				Map.Entry<byte[], Pair<byte[], RequestOption>> entry = i.next();
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				s += (40 + (pkey.length + skey.length + PREFIX_KEY_TYPE.length));
				s += (40 + value.first().length);
			}
		}
		return s;
	}

	public static PrefixPutMultiRequest build(short ns, byte[] pkey,
			Map<byte[], Pair<byte[], RequestOption>> keyValuePairs,
			Map<byte[], Pair<byte[], RequestOption>> keyCounterPairs, CompressOption compressOpt) {
		if (ns < 0 || ns >= NkvConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(NkvConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > NkvConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		if (keyValuePairs == null && keyCounterPairs == null) {
			throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
		}
		boolean isCompressedEnabled = compressOpt.isCompressEnabled();
		Map<byte[], Pair<CompressedValue, RequestOption>> ckvs = null;
		if (keyValuePairs != null) {
			if (keyValuePairs.size() == 0) {
				throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
			}
			for (Iterator<Entry<byte[], Pair<byte[], RequestOption>>> it = keyValuePairs.entrySet().iterator(); it.hasNext(); ) {
				Entry<byte[], Pair<byte[], RequestOption>> entry = it.next();
				byte[] skey = entry.getKey();
				if (skey == null
						|| (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > NkvConstant.MAX_KEY_SIZE) {
					throw new IllegalArgumentException(
							NkvConstant.KEY_NOT_AVAILABLE);
				}
				if (entry.getValue() == null) {
					throw new IllegalArgumentException(
							NkvConstant.VALUE_NOT_AVAILABLE);
				}
				byte[] v = entry.getValue().first();
				RequestOption ro = entry.getValue().second();
				if (v == null || ro == null) {
					throw new IllegalArgumentException(
							NkvConstant.VALUE_NOT_AVAILABLE);
				}
				if (isCompressedEnabled && v.length > compressOpt.getCompressThreshold()) {
					CompressedValue cv = NkvUtil.compress(v, compressOpt.isUseFastCompress());
					//检查压缩过的数据是否依旧超限
					if (cv.checkOverFlow(NkvConstant.MAX_VALUE_SIZE))
						throw new IllegalArgumentException(
								NkvConstant.VALUE_NOT_AVAILABLE);
					if (ckvs == null)
						ckvs = new HashMap<byte[], Pair<CompressedValue, RequestOption>>();
					ckvs.put(skey, new Pair<CompressedValue, RequestOption>(cv, ro));
					it.remove();
				}
			}
		}
		if (keyCounterPairs != null) {
			if (keyCounterPairs.size() == 0) {
				throw new IllegalArgumentException(NkvConstant.VALUE_NOT_AVAILABLE);
			}
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : keyCounterPairs
					.entrySet()) {
				byte[] skey = entry.getKey();
				if (skey == null
						|| (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > NkvConstant.MAX_KEY_SIZE) {
					throw new IllegalArgumentException(
							NkvConstant.KEY_NOT_AVAILABLE);
				}
				if (entry.getValue() == null) {
					throw new IllegalArgumentException(
							NkvConstant.VALUE_NOT_AVAILABLE);
				}
				if (entry.getValue().first() == null
						|| entry.getValue().second() == null) {
					throw new IllegalArgumentException(
							NkvConstant.VALUE_NOT_AVAILABLE);
				}
			}
		}
		PrefixPutMultiRequest request = new PrefixPutMultiRequest(ns, pkey,
				keyValuePairs, keyCounterPairs);
		if (ckvs != null)
			request.ckvs = ckvs;
		return request;
	}
	public short getNamespace() {
		return this.namespace;
	}
}
