package com.netease.backend.nkv.client.rpc.protocol.tair2_3;

import java.util.HashMap;

import com.netease.backend.nkv.client.packets.common.BatchReturnResponse;
import com.netease.backend.nkv.client.packets.common.PingRequest;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
import com.netease.backend.nkv.client.packets.configserver.GetGroupRequest;
import com.netease.backend.nkv.client.packets.configserver.GetGroupResponse;
import com.netease.backend.nkv.client.packets.configserver.QueryInfoRequest;
import com.netease.backend.nkv.client.packets.configserver.QueryInfoResponse;
import com.netease.backend.nkv.client.packets.dataserver.BatchPutRequest;
//import com.netease.backend.nkv.client.packets.dataserver.BoundedIncDecRequest;
//import com.netease.backend.nkv.client.packets.dataserver.BoundedIncDecResponse;
//import com.netease.backend.nkv.client.packets.dataserver.BoundedPrefixIncDecRequest;
//import com.netease.backend.nkv.client.packets.dataserver.BoundedPrefixIncDecResponse;
import com.netease.backend.nkv.client.packets.dataserver.DeleteRequest;
import com.netease.backend.nkv.client.packets.dataserver.DumpAllRequest;
import com.netease.backend.nkv.client.packets.dataserver.DumpAllResponse;
import com.netease.backend.nkv.client.packets.dataserver.DumpKeyRequest;
import com.netease.backend.nkv.client.packets.dataserver.DumpKeyResponse;
//import com.netease.backend.nkv.client.packets.dataserver.ExpireRequest;
import com.netease.backend.nkv.client.packets.dataserver.GetHiddenRequest;
import com.netease.backend.nkv.client.packets.dataserver.GetRequest;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.packets.dataserver.HideRequest;
import com.netease.backend.nkv.client.packets.dataserver.IncDecRequest;
import com.netease.backend.nkv.client.packets.dataserver.IncDecResponse;
import com.netease.backend.nkv.client.packets.dataserver.LockRequest;
import com.netease.backend.nkv.client.packets.dataserver.PrefixDeleteMultiRequest;
import com.netease.backend.nkv.client.packets.dataserver.PrefixGetHiddenMultiRequest;
import com.netease.backend.nkv.client.packets.dataserver.PrefixGetMultiRequest;
import com.netease.backend.nkv.client.packets.dataserver.PrefixGetMultiResponse;
import com.netease.backend.nkv.client.packets.dataserver.PrefixHideMultiRequest;
import com.netease.backend.nkv.client.packets.dataserver.PrefixIncDecRequest;
import com.netease.backend.nkv.client.packets.dataserver.PrefixIncDecResponse;
import com.netease.backend.nkv.client.packets.dataserver.PrefixPutMultiRequest;
import com.netease.backend.nkv.client.packets.dataserver.PutIfNoExistRequest;
import com.netease.backend.nkv.client.packets.dataserver.PutRequest;
//import com.netease.backend.nkv.client.packets.dataserver.RangeRequest;
//import com.netease.backend.nkv.client.packets.dataserver.RangeResponse;
//import com.netease.backend.nkv.client.packets.dataserver.SimplePrefixGetMultiRequest;
import com.netease.backend.nkv.client.packets.dataserver.SimplePrefixGetMultiResponse;
import com.netease.backend.nkv.client.packets.dataserver.TouchRequest;
import com.netease.backend.nkv.client.packets.dataserver.TrafficCheckRequest;
import com.netease.backend.nkv.client.packets.dataserver.TrafficCheckResponse;
import com.netease.backend.nkv.client.packets.invalidserver.HideByProxyMultiRequest;
import com.netease.backend.nkv.client.packets.invalidserver.HideByProxyRequest;
import com.netease.backend.nkv.client.packets.invalidserver.InvalidByProxyMultiRequest;
import com.netease.backend.nkv.client.packets.invalidserver.InvalidByProxyRequest;

//管理packet class与packet code之间的映射关系
public class PacketManager {
	
	private static final int REQ_PUT    = 1;
	private static final int REQ_MPUT = 2129;
	private static final int REQ_GET    = 2;

	private static final int REQ_DELETE = 3;
	private static final int REQ_INCDEC = 11;
	private static final int RESP_INCDEC = 105;
	private static final int REQ_LOCK = 14;
	//private static final int REQ_EXPIRE = 2127;
	
 
	private static final int REQ_PING 	= 6;
	
	//private static final int REQ_RANGE = 18;
	//private static final int RESP_RANGE = 19;
	private static final int REQ_HIDE = 20;
	private static final int REQ_HIDE_PROXY = 21;
	private static final int REQ_GET_HIDDEN = 22;
	private static final int REQ_INVAL_PROXY = 23;
	private static final int REQ_INVAL_PREFIX_HIDE_MULTI_BY_PROXY = 33;
	private static final int REQ_INVAL_PREFIX_INVALID_MULTI_BY_PROXY = 32;
	
	
	private static final int REQ_PREFIX_PUTS = 24;
	private static final int REQ_PREFIX_GETS = 29;
	private static final int RESP_PREFIX_GETS = 30;
	private static final int REQ_PREFIX_DELETES = 25;
	private static final int REQ_PREFIX_HIDES = 31;
	private static final int REQ_PREFIX_GET_HIDDENS = 34;
	
	private static final int RESP_GET   = 102;
	private static final int RET_PCK 	= 101;
	private static final int RESP_BATCH_RETURN = 28;

	private static final int REQ_GET_GROUP = 1002;
	private static final int RESP_GET_GROUP = 1102;
	private static final int REQ_PREFIX_INCDEC = 26;
	private static final int RESP_PREFIX_INCDEC = 27;
	//private static final int REQ_BOUNDED_INCDEC = 1704;
	//private static final int RESP_BOUNDED_INCDEC = 1705;
	//private static final int REQ_BOUNDED_PREFIX_INCDEC = 1706;
	//private static final int RESP_BOUNDED_PREFIX_INCDEC = 1707;
	
	private static final int TAIR_REQ_QUERY_INFO = 1009;
	private static final int TAIR_RESP_QUERY_INFO = 1106;
	//private static final int TAIR_REQ_SIMPLE_GET = 36;
	private static final int TAIR_RESP_SIMPLE_GET = 37;
	
	private static final int TAIR_FLOW_CONTROL = 9001;
	private static final int TAIR_FLOW_CHECK = 9005;
	
	
	//hset
	public final static int TAIR_REQ_HGETALL_PACKET = 2107;
    public final static int TAIR_RESP_HGETALL_PACKET = 2207;

    public final static int TAIR_REQ_HINCRBY_PACKET = 2108;
    public final static int TAIR_RESP_HINCRBY_PACKET = 2208;

    public final static int TAIR_REQ_HMSET_PACKET = 2109;
    public final static int TAIR_RESP_HMSET_PACKET = 2209;

    public final static int TAIR_REQ_HSET_PACKET = 2110;
    public final static int TAIR_RESP_HSET_PACKET = 2210;

    public final static int TAIR_REQ_HSETNX_PACKET = 2111;
    public final static int TAIR_RESP_HSETNX_PACKET = 2211;

    public final static int TAIR_REQ_HGET_PACKET = 2112;
    public final static int TAIR_RESP_HGET_PACKET = 2212;

    public final static int TAIR_REQ_HMGET_PACKET = 2113;
    public final static int TAIR_RESP_HMGET_PACKET = 2213;

    public final static int TAIR_REQ_HVALS_PACKET = 2114;
    public final static int TAIR_RESP_HVALS_PACKET = 2214;

    public final static int TAIR_REQ_HDEL_PACKET = 2115;
    public final static int TAIR_RESP_HDEL_PACKET = 2215;

    public final static int TAIR_REQ_HLEN_PACKET = 2136;
    public final static int TAIR_RESP_HLEN_PACKET = 2236;

    public final static int TAIR_REQ_HEXISTS_PACKET = 2161;
    
   //list
    public final static int TAIR_REQ_LPOP_PACKET = 2100;
    public final static int TAIR_RESP_LPOP_PACKET = 2200;

    public final static int TAIR_REQ_LPUSH_PACKET = 2101;
    public final static int TAIR_RESP_LPUSH_PACKET = 2201;

    public final static int TAIR_REQ_RPOP_PACKET = 2102;
    public final static int TAIR_RESP_RPOP_PACKET = 2202;

    public final static int TAIR_REQ_RPUSH_PACKET = 2103;
    public final static int TAIR_RESP_RPUSH_PACKET = 2203;

    public final static int TAIR_REQ_LPUSHX_PACKET = 2104;
    public final static int TAIR_RESP_LPUSHX_PACKET = 2204;

    public final static int TAIR_REQ_RPUSHX_PACKET = 2105;
    public final static int TAIR_RESP_RPUSHX_PACKET = 2205;

    public final static int TAIR_REQ_LINDEX_PACKET = 2106;
    public final static int TAIR_RESP_LINDEX_PACKET = 2206;

    public final static int TAIR_REQ_LTRIM_PACKET = 2128;
    public final static int TAIR_RESP_LTRIM_PACKET = 2228;

//    public final static int TAIR_REQ_LREM_PACKET = 2129;
//    public final static int TAIR_RESP_LREM_PACKET = 2229;

    public final static int TAIR_REQ_LRANGE_PACKET = 2130;
    public final static int TAIR_RESP_LRANGE_PACKET = 2230;

    public final static int TAIR_REQ_LLEN_PACKET = 2133;
    public final static int TAIR_RESP_LLEN_PACKET = 2233;

    public final static int TAIR_REQ_LPUSH_LIMIT_PACKET = 2157;
    public final static int TAIR_REQ_RPUSH_LIMIT_PACKET = 2158;
    public final static int TAIR_REQ_LPUSHX_LIMIT_PACKET = 2159;
    public final static int TAIR_REQ_RPUSHX_LIMIT_PACKET = 2160;
    
    //set
    public final static int TAIR_REQ_SCARD_PACKET = 2116;
    public final static int TAIR_RESP_SCARD_PACKET = 2216;

    public final static int TAIR_REQ_SMEMBERS_PACKET = 2117;
    public final static int TAIR_RESP_SMEMBERS_PACKET = 2217;

    public final static int TAIR_REQ_SADD_PACKET = 2118;
    public final static int TAIR_RESP_SADD_PACKET = 2218;

    public final static int TAIR_REQ_SPOP_PACKET = 2119;
    public final static int TAIR_RESP_SPOP_PACKET = 2219;

    public final static int TAIR_REQ_SREM_PACKET = 2145;
    public final static int TAIR_RESP_SREM_PACKET = 2245;

    public final static int TAIR_REQ_SADDMULTI_PACKET = 2146;
    public final static int TAIR_RESP_SADDMULTI_PACKET = 2246;
    
    public final static int TAIR_REQ_SREMMULTI_PACKET = 2147;
    public final static int TAIR_RESP_SREMMULTI_PACKET = 2247;
    
    public final static int TAIR_REQ_SMEMBERSMULTI_PACKET = 2148;
    public final static int TAIR_RESP_SMEMBERSMULTI_PACKET = 2248;
    
    //zset
    public final static int TAIR_REQ_ZRANGE_PACKET = 2120;
    public final static int TAIR_RESP_ZRANGE_PACKET = 2220;

    public final static int TAIR_REQ_ZREVRANGE_PACKET = 2121;
    public final static int TAIR_RESP_ZREVRANGE_PACKET = 2221;

    public final static int TAIR_REQ_ZSCORE_PACKET = 2122;
    public final static int TAIR_RESP_ZSCORE_PACKET = 2222;

    public final static int TAIR_REQ_ZRANGEBYSCORE_PACKET = 2123;
    public final static int TAIR_RESP_ZRANGEBYSCORE_PACKET = 2223;

    public final static int TAIR_REQ_ZADD_PACKET = 2124;
    public final static int TAIR_RESP_ZADD_PACKET = 2224;

    public final static int TAIR_REQ_ZRANK_PACKET = 2125;
    public final static int TAIR_RESP_ZRANK_PACKET = 2225;

    public final static int TAIR_REQ_ZCARD_PACKET = 2126;
    public final static int TAIR_RESP_ZCARD_PACKET = 2226;

    public final static int TAIR_REQ_ZREM_PACKET = 2137;
    public final static int TAIR_RESP_ZREM_PACKET = 2237;

    public final static int TAIR_REQ_ZREMRANGEBYRANK_PACKET = 2138;
    public final static int TAIR_RESP_ZREMRANGEBYRANK_PACKET = 2238;

    public final static int TAIR_REQ_ZREMRANGEBYSCORE_PACKET = 2139;
    public final static int TAIR_RESP_ZREMRANGEBYSCORE_PACKET = 2239;

    public final static int TAIR_REQ_ZREVRANK_PACKET = 2140;
    public final static int TAIR_RESP_ZREVRANK_PACKET = 2240;

    public final static int TAIR_REQ_ZCOUNT_PACKET = 2141;
    public final static int TAIR_RESP_ZCOUNT_PACKET = 2241;

    public final static int TAIR_REQ_ZINCRBY_PACKET = 2142;
    public final static int TAIR_RESP_ZINCRBY_PACKET = 2242;

    public final static int TAIR_RESP_ZRANGEWITHSCORE_PACKET = 2243;
    public final static int TAIR_RESP_ZREVRANGEWITHSCORE_PACKET = 2244;

    public final static int TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET = 2151;
    public final static int TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET = 2251;
    
    //common
    public final static int TAIR_REQ_EXISTS_PACKET = 2153;
    
    public final static int TAIR_REQ_EXPIRE_PACKET = 2127;
    public final static int TAIR_RESP_EXPIRE_PACKET = 2227;

    public final static int TAIR_REQ_EXPIREAT_PACKET = 2131;
    public final static int TAIR_RESP_EXPIREAT_PACKET = 2231;

    public final static int TAIR_REQ_PERSIST_PACKET = 2132;
    public final static int TAIR_RESP_PERSIST_PACKET = 2232;

    public final static int TAIR_REQ_TTL_PACKET = 2134;
    public final static int TAIR_RESP_TTL_PACKET = 2234;

    public final static int TAIR_REQ_TYPE_PACKET = 2135;
    public final static int TAIR_RESP_TYPE_PACKET = 2235;

    public final static int TAIR_REQ_ADD_FILTER_PACKET = 2162;
    
    public final static int TAIR_REQ_REMOVE_FILTER_PACKET = 2163;
    
    public final static int TAIR_REQ_DUMP_AREA_PACKET = 2164;
    public final static int TAIR_REQ_LOAD_AREA_PACKET = 2165;
    
    public final static int TAIR_REQ_SET_NS_ATTR_PACKET = 2166;
    public final static int TAIR_REQ_GET_NS_ATTR_PACKET = 2167;
    
    //for key dump and all dump
	private static final int REQ_DUMPKEY = 20001;
	private static final int RESP_DUMPKEY = 20101;
	private static final int REQ_TOUCH = 20002;
	private static final int REQ_PUT_IF_NOEXIST = 20003;
	
	private static final int REQ_DUMPALL = 20000;
	private static final int RESP_DUMPALL = 20100;

	private static HashMap<Integer, Class<? extends Packet>> codePacketMap 
		= new HashMap<Integer, Class<? extends Packet>>();
	
	private static HashMap<Class<? extends Packet>, Integer> packetCodeMap 
		= new HashMap<Class<? extends Packet>, Integer>();
	
	static {
		PacketManager.regist(REQ_GET, GetRequest.class);
		PacketManager.regist(RESP_GET, GetResponse.class);
		PacketManager.regist(REQ_PUT, PutRequest.class);
		PacketManager.regist(REQ_PUT_IF_NOEXIST, PutIfNoExistRequest.class);
		PacketManager.regist(RET_PCK, ReturnResponse.class);
		PacketManager.regist(REQ_GET_GROUP, GetGroupRequest.class);
		PacketManager.regist(RESP_GET_GROUP, GetGroupResponse.class);
		PacketManager.regist(REQ_INVAL_PROXY, InvalidByProxyRequest.class);
		PacketManager.regist(REQ_HIDE_PROXY, HideByProxyRequest.class);
		PacketManager.regist(REQ_INVAL_PREFIX_HIDE_MULTI_BY_PROXY, HideByProxyMultiRequest.class);
		PacketManager.regist(REQ_INVAL_PREFIX_INVALID_MULTI_BY_PROXY, InvalidByProxyMultiRequest.class);
		PacketManager.regist(REQ_PING, PingRequest.class);
		PacketManager.regist(REQ_MPUT, BatchPutRequest.class);
		//PacketManager.regist(REQ_EXPIRE, ExpireRequest.class);
		PacketManager.regist(REQ_DELETE, DeleteRequest.class);
		PacketManager.regist(REQ_INCDEC, IncDecRequest.class);
		PacketManager.regist(RESP_INCDEC, IncDecResponse.class);
		PacketManager.regist(REQ_LOCK, LockRequest.class);
		PacketManager.regist(REQ_HIDE, HideRequest.class);
		PacketManager.regist(REQ_GET_HIDDEN, GetHiddenRequest.class);
		PacketManager.regist(REQ_PREFIX_PUTS, PrefixPutMultiRequest.class);
		PacketManager.regist(REQ_PREFIX_GETS, PrefixGetMultiRequest.class);
		PacketManager.regist(RESP_PREFIX_GETS, PrefixGetMultiResponse.class);
		PacketManager.regist(REQ_PREFIX_DELETES, PrefixDeleteMultiRequest.class);
		PacketManager.regist(RESP_BATCH_RETURN, BatchReturnResponse.class);
		PacketManager.regist(REQ_PREFIX_HIDES, PrefixHideMultiRequest.class);
		PacketManager.regist(REQ_PREFIX_GET_HIDDENS, PrefixGetHiddenMultiRequest.class);
		PacketManager.regist(REQ_PREFIX_INCDEC, PrefixIncDecRequest.class);
		PacketManager.regist(RESP_PREFIX_INCDEC, PrefixIncDecResponse.class);
		//PacketManager.regist(REQ_RANGE, RangeRequest.class);
		//PacketManager.regist(RESP_RANGE, RangeResponse.class);
		//PacketManager.regist(REQ_BOUNDED_INCDEC, BoundedIncDecRequest.class);
		//PacketManager.regist(RESP_BOUNDED_INCDEC, BoundedIncDecResponse.class);
		//PacketManager.regist(REQ_BOUNDED_PREFIX_INCDEC, BoundedPrefixIncDecRequest.class);
		//PacketManager.regist(RESP_BOUNDED_PREFIX_INCDEC, BoundedPrefixIncDecResponse.class);
		PacketManager.regist(TAIR_REQ_QUERY_INFO, QueryInfoRequest.class);
		PacketManager.regist(TAIR_RESP_QUERY_INFO, QueryInfoResponse.class);
		//PacketManager.regist(TAIR_REQ_SIMPLE_GET, SimplePrefixGetMultiRequest.class);
		PacketManager.regist(TAIR_RESP_SIMPLE_GET, SimplePrefixGetMultiResponse.class);
		PacketManager.regist(TAIR_FLOW_CONTROL, TrafficCheckResponse.class);
		PacketManager.regist(TAIR_FLOW_CHECK, TrafficCheckRequest.class);
		
		/*
		//hset
		PacketManager.regist(TAIR_REQ_HSET_PACKET, RequestHSetPacket.class);
		PacketManager.regist(TAIR_RESP_HSET_PACKET, ResponseHSetPacket.class);
		PacketManager.regist(TAIR_REQ_HGET_PACKET, RequestHGetPacket.class);
		PacketManager.regist(TAIR_RESP_HGET_PACKET, ResponseHGetPacket.class);
		PacketManager.regist(TAIR_REQ_HDEL_PACKET, RequestHDelPacket.class);
		PacketManager.regist(TAIR_RESP_HDEL_PACKET, ResponseHDelPacket.class);
		PacketManager.regist(TAIR_REQ_HLEN_PACKET, RequestHLenPacket.class);
		PacketManager.regist(TAIR_RESP_HLEN_PACKET, ResponseHLenPacket.class);
		PacketManager.regist(TAIR_REQ_HMGET_PACKET, RequestHMgetPacket.class);
		PacketManager.regist(TAIR_RESP_HMGET_PACKET, ResponseHMgetPacket.class);
		PacketManager.regist(TAIR_REQ_HMSET_PACKET, RequestHMsetPacket.class);
		PacketManager.regist(TAIR_RESP_HMSET_PACKET, ResponseHMsetPacket.class);
		PacketManager.regist(TAIR_REQ_HEXISTS_PACKET, RequestHExistsPacket.class);
		PacketManager.regist(TAIR_REQ_HGETALL_PACKET, RequestHGetAllPacket.class);
		PacketManager.regist(TAIR_RESP_HGETALL_PACKET, ResponseHGetAllPacket.class);
		PacketManager.regist(TAIR_REQ_HINCRBY_PACKET, RequestHIncrbyPacket.class);
		PacketManager.regist(TAIR_RESP_HINCRBY_PACKET, ResponseHIncrbyPacket.class);
		PacketManager.regist(TAIR_REQ_HVALS_PACKET, RequestHValsPacket.class);
		PacketManager.regist(TAIR_RESP_HVALS_PACKET, ResponseHValsPacket.class);
		
		//list
		PacketManager.regist(TAIR_REQ_LPOP_PACKET, RequestLPopPacket.class);
		PacketManager.regist(TAIR_RESP_LPOP_PACKET, ResponseLPopPacket.class);
	    PacketManager.regist(TAIR_REQ_LPUSH_PACKET, RequestLPushPacket.class);
	    PacketManager.regist(TAIR_RESP_LPUSH_PACKET, ResponseLPushPacket.class);
	    PacketManager.regist(TAIR_REQ_RPOP_PACKET, RequestRPopPacket.class);
	    PacketManager.regist(TAIR_RESP_RPOP_PACKET, ResponseRPopPacket.class);
	    PacketManager.regist(TAIR_REQ_RPUSH_PACKET, RequestRPushPacket.class);
	    PacketManager.regist(TAIR_RESP_RPUSH_PACKET, ResponseRPushPacket.class);
//	    PacketManager.regist(TAIR_REQ_LPUSHX_PACKET, 2104.class);
//	  	PacketManager.regist(TAIR_RESP_LPUSHX_PACKET, 2204.class);
//	  	PacketManager.regist(TAIR_REQ_RPUSHX_PACKET, 2105.class);
//	  	PacketManager.regist(TAIR_RESP_RPUSHX_PACKET, 2205.class);
	  	PacketManager.regist(TAIR_REQ_LINDEX_PACKET, RequestLIndexPacket.class);
	  	PacketManager.regist(TAIR_RESP_LINDEX_PACKET, ResponseLIndexPacket.class);
	  	PacketManager.regist(TAIR_REQ_LTRIM_PACKET, RequestLTrimPacket.class);
	  	PacketManager.regist(TAIR_RESP_LTRIM_PACKET, ResponseLTrimPacket.class);
//	  	PacketManager.regist(TAIR_REQ_LREM_PACKET, RequestLRemPacket.class);
//	  	PacketManager.regist(TAIR_RESP_LREM_PACKET, ResponseLRemPacket.class);
	  	PacketManager.regist(TAIR_REQ_LRANGE_PACKET, RequestLRangePacket.class);
	  	PacketManager.regist(TAIR_RESP_LRANGE_PACKET, ResponseLRangePacket.class);
	  	PacketManager.regist(TAIR_REQ_LLEN_PACKET, RequestLLenPacket.class);
	  	PacketManager.regist(TAIR_RESP_LLEN_PACKET, ResponseLLenPacket.class);
	  	PacketManager.regist(TAIR_REQ_LPUSH_LIMIT_PACKET, RequestLPushLimitPacket.class);
	  	PacketManager.regist(TAIR_REQ_RPUSH_LIMIT_PACKET, RequestRPushLimitPacket.class);
//	  	PacketManager.regist(TAIR_REQ_LPUSHX_LIMIT_PACKET, RequestLPushLimitPacket.class);
//	  	PacketManager.regist(TAIR_REQ_RPUSHX_LIMIT_PACKET, 2160.class);
	  	
	  	//set
	  	PacketManager.regist(TAIR_REQ_SCARD_PACKET, RequestSCardPacket.class);
	  	PacketManager.regist(TAIR_RESP_SCARD_PACKET, ResponseSCardPacket.class);

	  	PacketManager.regist(TAIR_REQ_SMEMBERS_PACKET, RequestSMembersPacket.class);
	  	PacketManager.regist(TAIR_RESP_SMEMBERS_PACKET, ResponseSMembersPacket.class);

	  	PacketManager.regist(TAIR_REQ_SADD_PACKET, RequestSAddPacket.class);
	  	PacketManager.regist(TAIR_RESP_SADD_PACKET, ResponseSAddPacket.class);

	  	PacketManager.regist(TAIR_REQ_SPOP_PACKET, RequestSPopPacket.class);
	  	PacketManager.regist(TAIR_RESP_SPOP_PACKET, ResponseSPopPacket.class);

	  	PacketManager.regist(TAIR_REQ_SREM_PACKET, RequestSRemPacket.class);
	  	PacketManager.regist(TAIR_RESP_SREM_PACKET, ResponseSRemPacket.class);

	  	PacketManager.regist(TAIR_REQ_SADDMULTI_PACKET, RequestSAddMultiPacket.class);
	  	PacketManager.regist(TAIR_RESP_SADDMULTI_PACKET, ResponseSAddMultiPacket.class);

	  	PacketManager.regist(TAIR_REQ_SREMMULTI_PACKET, RequestSRemMultiPacket.class);
	  	PacketManager.regist(TAIR_RESP_SREMMULTI_PACKET, ResponseSRemMultiPacket.class);

	  	PacketManager.regist(TAIR_REQ_SMEMBERSMULTI_PACKET, RequestSMembersMultiPacket.class);
	  	PacketManager.regist(TAIR_RESP_SMEMBERSMULTI_PACKET, ResponseSMembersMultiPacket.class);
	  	
	    //zset
	    PacketManager.regist(TAIR_REQ_ZRANGE_PACKET, RequestZRangePacket.class);
	    PacketManager.regist(TAIR_RESP_ZRANGE_PACKET, ResponseZRangePacket.class);

	    PacketManager.regist(TAIR_REQ_ZREVRANGE_PACKET, RequestZRevrangePacket.class);
	    PacketManager.regist(TAIR_RESP_ZREVRANGE_PACKET, ResponseZRevrangePacket.class);

	    PacketManager.regist(TAIR_REQ_ZSCORE_PACKET, RequestZScorePacket.class);
	    PacketManager.regist(TAIR_RESP_ZSCORE_PACKET, ResponseZScorePacket.class);

	    PacketManager.regist(TAIR_REQ_ZRANGEBYSCORE_PACKET, RequestZRangebyScorePacket.class);
	    PacketManager.regist(TAIR_RESP_ZRANGEBYSCORE_PACKET, ResponseZRangebyScorePacket.class);

	    PacketManager.regist(TAIR_REQ_ZADD_PACKET, RequestZAddPacket.class);
	    PacketManager.regist(TAIR_RESP_ZADD_PACKET, ResponseZAddPacket.class);

	    PacketManager.regist(TAIR_REQ_ZRANK_PACKET, RequestZRankPacket.class);
	    PacketManager.regist(TAIR_RESP_ZRANK_PACKET, ResponseZRankPacket.class);

	    PacketManager.regist(TAIR_REQ_ZCARD_PACKET, RequestZCardPacket.class);
	    PacketManager.regist(TAIR_RESP_ZCARD_PACKET, ResponseZCardPacket.class);

	    PacketManager.regist(TAIR_REQ_ZREM_PACKET, RequestZRemPacket.class);
	    PacketManager.regist(TAIR_RESP_ZREM_PACKET, ResponseZRemPacket.class);

	    PacketManager.regist(TAIR_REQ_ZREMRANGEBYRANK_PACKET, RequestZRemrangebyRankPacket.class);
	    PacketManager.regist(TAIR_RESP_ZREMRANGEBYRANK_PACKET, ResponseZRemrangebyRankPacket.class);

	    PacketManager.regist(TAIR_REQ_ZREMRANGEBYSCORE_PACKET, RequestZRemrangebyScorePacket.class);
	    PacketManager.regist(TAIR_RESP_ZREMRANGEBYSCORE_PACKET, ResponseZRemrangebyScorePacket.class);

	    PacketManager.regist(TAIR_REQ_ZREVRANK_PACKET, RequestZRevrankPacket.class);
	    PacketManager.regist(TAIR_RESP_ZREVRANK_PACKET, ResponseZRevrankPacket.class);

	    PacketManager.regist(TAIR_REQ_ZCOUNT_PACKET, RequestZCountPacket.class);
	    PacketManager.regist(TAIR_RESP_ZCOUNT_PACKET, ResponseZCountPacket.class);

	    PacketManager.regist(TAIR_REQ_ZINCRBY_PACKET, RequestZIncrbyPacket.class);
	    PacketManager.regist(TAIR_RESP_ZINCRBY_PACKET, ResponseZIncrbyPacket.class);

	    PacketManager.regist(TAIR_RESP_ZRANGEWITHSCORE_PACKET, RequestZRangeWithScorePacket.class);
	    PacketManager.regist(TAIR_RESP_ZREVRANGEWITHSCORE_PACKET, ResponseZRangeWithScorePacket.class);

	    PacketManager.regist(TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET, RequestGenericZRangeByScorePacket.class);
	    PacketManager.regist(TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET, ResponseGenericZRangeByScorePacket.class);
	    
	    //COMMON
	    PacketManager.regist(TAIR_REQ_EXPIRE_PACKET, RequestExpirePacket.class);
	    PacketManager.regist(TAIR_RESP_EXPIRE_PACKET, ResponseExpirePacket.class);
	    PacketManager.regist(TAIR_REQ_TTL_PACKET, RequestTTLPacket.class);
	    PacketManager.regist(TAIR_RESP_TTL_PACKET, ResponseTTLPacket.class);
	    PacketManager.regist(TAIR_REQ_TYPE_PACKET, RequestTypePacket.class);
	    PacketManager.regist(TAIR_RESP_TYPE_PACKET, ResponseTypePacket.class);
	    PacketManager.regist(TAIR_REQ_EXISTS_PACKET, RequestExistsPacket.class);
	    */
	    
	    //dump
	    PacketManager.regist(REQ_DUMPKEY, DumpKeyRequest.class);
	    PacketManager.regist(RESP_DUMPKEY, DumpKeyResponse.class);
	    PacketManager.regist(REQ_DUMPALL, DumpAllRequest.class);
	    PacketManager.regist(RESP_DUMPALL, DumpAllResponse.class);
	    PacketManager.regist(REQ_TOUCH, TouchRequest.class);
	}
	
	public static void regist(Integer packetCode, Class<? extends Packet> cls) {
		if (codePacketMap.containsKey(packetCode) || packetCodeMap.containsKey(cls))
			throw new IllegalArgumentException("Packet Code " + packetCode + " already exists");
		codePacketMap.put(packetCode, cls);
		packetCodeMap.put(cls, packetCode);
	}
	
	public static Integer getPacketCode(Class<? extends Packet> cls) {
		return packetCodeMap.get(cls);
	}
	
	public static Class<? extends Packet> getPacketClass(Integer code) {
		return codePacketMap.get(code);
	}
}
