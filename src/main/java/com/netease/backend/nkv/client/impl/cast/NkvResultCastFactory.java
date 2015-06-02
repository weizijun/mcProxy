package com.netease.backend.nkv.client.impl.cast;

public class NkvResultCastFactory {
	public static AddCountCast ADD_COUNT = new  AddCountCast();
	public static AddCountBoundedCast ADD_COUNT_BOUNDED = new  AddCountBoundedCast();
	public static BatchDeleteCast BATCH_DELETE = new BatchDeleteCast(); 
	public static BatchGetCast BATCH_GET = new BatchGetCast();
	public static BatchInvalidCast BATCH_INVALID = new BatchInvalidCast();
	public static BatchLockKeyCast BATCH_LOCK_KEY = new BatchLockKeyCast();
	public static BatchPrefixGetMultiCast  BATCH_PREFIX_GET_MULTI = new BatchPrefixGetMultiCast();
	public static BatchPrefixGetHiddenMultiCast BATCH_PREFIX_GET_HIDDEN_MULTI = new BatchPrefixGetHiddenMultiCast();
	public static BatchPutCast BATCH_PUT =  new BatchPutCast();
	public static BatchPutOldCast BATCH_PUT_OLD = new BatchPutOldCast();
	public static DeleteCast DELETE =  new DeleteCast();
	public static ExpireCast EXPIRE =  new ExpireCast();
	public static GetCast GET = new GetCast();
	public static GetHiddenCast GET_HIDDEN = new GetHiddenCast();
	//public static GetRangeCast GET_RANGE = new GetRangeCast();
	public static HideByProxyCast HIDE_BY_PROXY = new HideByProxyCast();
	public static HideCast HIDE = new HideCast();
	public static InvalidCast INVALID = new InvalidCast();
	public static LockKeyCast LOCK_KEY = new LockKeyCast();
	public static PrefixAddCountMultiCast PREFIX_ADD_COUNT_MULTI =  new PrefixAddCountMultiCast();
	public static PrefixAddCountBoundedMultiCast PREFIX_ADD_COUNT_BOUNDED_MULTI =  new PrefixAddCountBoundedMultiCast();
	public static PrefixDeleteMultiCast PREFIX_DELETE_MULTI =  new PrefixDeleteMultiCast();
	public static PrefixGetHiddenMultiCast PREFIX_GET_HIDDEN_MULTI =  new PrefixGetHiddenMultiCast();
	public static PrefixGetMultiCast PREFIX_GET_MULTI = new PrefixGetMultiCast();
	public static PrefixHideMultiByProxyCast PREFIX_HIDE_MULTI_BY_PROXY = new PrefixHideMultiByProxyCast();
	public static PrefixHideMultiCast PREFIX_HIDE_MULTI = new PrefixHideMultiCast();
	public static PrefixInvalidMultiCast PREFIX_INVALID_MULTI = new PrefixInvalidMultiCast();
	public static PrefixPutMultiCast PREFIX_PUT_MULTI = new PrefixPutMultiCast();
	//public static GetRangeKeyCast GET_RANGE_KEY = new GetRangeKeyCast();
	//public static GetRangeValueCast GET_RANGE_VALUE = new GetRangeValueCast();
	public static PutCast PUT = new PutCast();
	public static QueryInfoCast QUERY_INFO = new QueryInfoCast();
	public static SimplePrefixGetMultiCast SIMPLE_PREFIX_GET_MULTI = new SimplePrefixGetMultiCast();
	
	/*
	//for rdb hset
	public static HGetAllCast HGET_ALL = new HGetAllCast();
	public static LongValueCast HINC_BY = new LongValueCast();
	public static HMsetCast HMSET = new HMsetCast();
	public static HGetCast HGET = new HGetCast();
	public static ByteListCast HVALS = new ByteListCast();
	public static HMgetCast HMGET = new HMgetCast();
	
	//for rdb list
	public static LIndexGetCast LINDEX_GET = new LIndexGetCast();
	public static LPushCast LPUSH = new LPushCast();
	public static LRPushCast RPUSH = new LRPushCast();
	
	//for rdb set
	public static SAddMultiCast SADD_MULTI = new SAddMultiCast();
	public static SMembersCast SMEMBERS = new SMembersCast();
	public static SMembersMultiCast SMEMBERS_MULTI = new SMembersMultiCast();
	public static SRemMultiCast SREM_MULTI = new SRemMultiCast();
	public static SPopCast SPOP = new SPopCast();
	
	//for rdb zset
	public static ZScoreCast ZSCORE = new ZScoreCast();
	public static ZIncrbyCast ZINCRBY = new ZIncrbyCast();
	public static ZRangeCast ZRANGE = new ZRangeCast();
	public static ZRangeWithScoreCast ZRANGE_WITHSCORE = new ZRangeWithScoreCast();
	public static ZRangeGenericByScoreCast ZRANGE_GEN_BYSCORE = new ZRangeGenericByScoreCast();
//	public static ZRevrangebyScoreCast ZREVRANGE_BYSCORE = new ZRevrangebyScoreCast();
	public static ZRevrangeWithScoreCast ZREVRANGE_WITHSCORE = new ZRevrangeWithScoreCast();
	public static ZRevrangeCast ZREVRANGE = new ZRevrangeCast();
	public static ZRemRangeByRankCast ZREM_BYRANK = new ZRemRangeByRankCast();
	public static ZRemRangeByScoreCast ZREM_BYSCORE = new ZRemRangeByScoreCast();
	
	//for common
	public static SimpleCast SIMPLE = new SimpleCast();
	public static LongValueCast LONG_VALUE = new LongValueCast();
	public static ByteListCast BYTE_LIST = new ByteListCast();
	public static TypeCast TYPE = new TypeCast();*/
	
	//for dump
	public static DumpKeyCast DUMP_KEY = new DumpKeyCast();
	public static DumpAllCast DUMP_ALL = new DumpAllCast();
}
