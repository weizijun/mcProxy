package com.netease.backend.nkv.client.rpc.net;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.packets.dataserver.TrafficCheckRequest;


public class FlowLimit {
	private Logger log = LoggerFactory.getLogger(FlowLimit.class);
	
	private int threshold;//阀值，如果flowRandom产生的随机数超过该阀值，则判断为over flow，否则即判断down
	private long lastTime;//最近一次更新threshold时间
	private long checkTime;//主动发出check请求的时间

	private static final double UP_FACTOR = 0.3;//threshold up增加因子
	private static final long UP_CHECKTIME = 10 * 1000; //10s，check up的最小时间间隔

	private static final long DOWN_CHECKTIME = 5 * 1000; //5s，check down的最小时间间隔
	private static final double DOWN_FACTOR = 0.5;//threshold down减少因子

	private static final int MAX_THRESHOLD = 1000;
	private static final Random flowRandom = new Random();
	
	private static final int TRAFFIC_CHECK_REQUEST_TIMEOUT = 500;//check request timeout
	private int namespace;
	
	public static enum FlowStatus {
		KEEP , UP, DOWN, UNKNOWN
	}
	
	public static enum FlowType {
		IN, OUT, OPS, UNKNOWN
	}
	
	public FlowLimit(int ns) {
		threshold = 0;
		lastTime = 0;
		checkTime = 0;
		namespace = ns;
	}
	
	public int getThreshold() {
		return threshold;
	}
	
	public boolean isOverflow() {
		
		if (threshold == 0)
			return false;
		else if (threshold >= MAX_THRESHOLD) {
			log.warn("Threshold " + threshold + " larger than max " + MAX_THRESHOLD);
			return true;
		} 
		//return false;
		 
		else
		{
			//这么做的好处是一旦被判定为over flow，不会限制所有的该namespace的request
			//而是以一定的概率限制request
			return flowRandom.nextInt(MAX_THRESHOLD) < threshold;
		}
		 
	}
	
	public boolean limitLevelUp() {
		long now = System.currentTimeMillis();
		if (now - lastTime < UP_CHECKTIME) {
			return false;
		}
		synchronized (this) {
			if (now - lastTime < UP_CHECKTIME) {
				return true;
			}
			//逐步提高被判断为over flow的概率
			if (threshold < MAX_THRESHOLD - 10)
				threshold = (int)(threshold + UP_FACTOR * (MAX_THRESHOLD - threshold));
			lastTime = now;
			log.warn("flow limit up ns " + namespace + " curt " + threshold);
		}
		return true;
	}
	
	public void limitLevelTouch() {
		lastTime = System.currentTimeMillis();
	}
	
	public boolean limitLevelDown() {
		long now = System.currentTimeMillis();
		if (now - lastTime < DOWN_CHECKTIME) {
			return false;
		}
		synchronized (this) {
			//在上一个if之后，lasttime被别的线程所更新
			if (now - lastTime < DOWN_CHECKTIME) {
				return true;
			}
			//这样做的好处是一旦前期被限流，不会因为一个down而导致全被解流，它是一个逐步解流的过程
			//一旦threshold阀值低于50，则全解流
			threshold = (int)(threshold - DOWN_FACTOR * threshold);
			if (threshold < 50)
				threshold = 0;
			lastTime = now;
			log.warn("flow limit down ns " + namespace + " curt " + threshold);
		}
		return true;
	}

	//请求流控
	public boolean limitLevelCheck(NkvRpcContext ctx, NkvRpcPacketFactory factory, NkvChannel channel, short ns) throws NkvRpcError {
		if (threshold == 0) {
			return false;
		}

		long now = System.currentTimeMillis();
		if (now - lastTime < DOWN_CHECKTIME || now - checkTime < DOWN_CHECKTIME) {
			return false;
		}
		synchronized (this) {
			//刚被别的线程更新lastTime或者刚发出check请求
			if (now - lastTime < DOWN_CHECKTIME || now - checkTime < DOWN_CHECKTIME) {
				return true;
			}
			checkTime = now;
		}
		log.warn("flow limit check ns " + namespace + " curt " + threshold);
		TrafficCheckRequest request = TrafficCheckRequest.build(ns);
		ctx.callAsync(channel, request, TRAFFIC_CHECK_REQUEST_TIMEOUT, factory);
		return true;
	}
}