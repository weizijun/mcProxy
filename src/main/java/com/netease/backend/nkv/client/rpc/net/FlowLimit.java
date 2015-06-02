package com.netease.backend.nkv.client.rpc.net;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.packets.dataserver.TrafficCheckRequest;


public class FlowLimit {
	private Logger log = LoggerFactory.getLogger(FlowLimit.class);
	
	private int threshold;//��ֵ�����flowRandom����������������÷�ֵ�����ж�Ϊover flow�������ж�down
	private long lastTime;//���һ�θ���thresholdʱ��
	private long checkTime;//��������check�����ʱ��

	private static final double UP_FACTOR = 0.3;//threshold up��������
	private static final long UP_CHECKTIME = 10 * 1000; //10s��check up����Сʱ����

	private static final long DOWN_CHECKTIME = 5 * 1000; //5s��check down����Сʱ����
	private static final double DOWN_FACTOR = 0.5;//threshold down��������

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
			//��ô���ĺô���һ�����ж�Ϊover flow�������������еĸ�namespace��request
			//������һ���ĸ�������request
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
			//����߱��ж�Ϊover flow�ĸ���
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
			//����һ��if֮��lasttime������߳�������
			if (now - lastTime < DOWN_CHECKTIME) {
				return true;
			}
			//�������ĺô���һ��ǰ�ڱ�������������Ϊһ��down������ȫ������������һ���𲽽����Ĺ���
			//һ��threshold��ֵ����50����ȫ����
			threshold = (int)(threshold - DOWN_FACTOR * threshold);
			if (threshold < 50)
				threshold = 0;
			lastTime = now;
			log.warn("flow limit down ns " + namespace + " curt " + threshold);
		}
		return true;
	}

	//��������
	public boolean limitLevelCheck(NkvRpcContext ctx, NkvRpcPacketFactory factory, NkvChannel channel, short ns) throws NkvRpcError {
		if (threshold == 0) {
			return false;
		}

		long now = System.currentTimeMillis();
		if (now - lastTime < DOWN_CHECKTIME || now - checkTime < DOWN_CHECKTIME) {
			return false;
		}
		synchronized (this) {
			//�ձ�����̸߳���lastTime���߸շ���check����
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