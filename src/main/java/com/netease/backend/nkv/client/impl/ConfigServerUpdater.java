package com.netease.backend.nkv.client.impl;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.packets.configserver.GetGroupResponse;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;
import com.netease.backend.nkv.client.rpc.net.NkvFuture.NkvFutureListener;

public class ConfigServerUpdater extends Thread {

	private boolean stop = false;
	
	protected static final Logger log = LoggerFactory.getLogger(ConfigServerUpdater.class);
	
	private static final int MAX_TASKS_COUNT = 100;
	
	private ConcurrentHashMap<ServerManager, Long> exists = new ConcurrentHashMap<ServerManager, Long>();
	
	private PriorityBlockingQueue<Task> tasks = new PriorityBlockingQueue<Task>(MAX_TASKS_COUNT * 3
			, new Comparator<Task>() {
				public int compare(Task o1, Task o2) {
					return o1.rank() - o2.rank();
				}
	});
	
	private interface Task {
		public int rank();
	}
	private class ResponseTask implements Task {
		RequestTask request;
		NkvResultFutureImpl<GetGroupResponse, Result<Void>> future;
		public int rank() {
			return request.rank();
		}			
	}
	
	private class RequestTask implements Task {
		ServerManager serverManager;
		private int	  createTime;
		
		public void updateCreateTime() {
			createTime = (int)(System.currentTimeMillis());
		}
		
		public int rank() {
			return createTime;
		}
	}
	
	public boolean submit(ServerManager sm) {
		if (exists.size() >= MAX_TASKS_COUNT) 
			return false;
		Long createTime = System.currentTimeMillis();
		Long lastTime = exists.put(sm, createTime);
		if (lastTime == null) {
			RequestTask request = new RequestTask();
			request.serverManager = sm;
			request.createTime	  = (int)(createTime / 1000);
			tasks.offer(request);
			return true;
		} 
		return false;
	}
	
	//向config-server获取集群信息
	//tair3-client是在fail次数超过一定阈值时向config-server获取集群信息。
	//tair-client的c和c++接口则是会定期向config-server获取集群信息
	@Override
	public void run() {
		while (Thread.interrupted() == false && stop == false) {
			try {
				Object obj = tasks.take();
				
				if (obj instanceof RequestTask) {
					doRequest((RequestTask)obj);
				} else if (obj instanceof ResponseTask) {
					doResponse((ResponseTask)obj);
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}
	
	private void doRequest(final RequestTask request) {
		ServerManager sm = request.serverManager;
		NkvResultFutureImpl<GetGroupResponse, Result<Void>> future;
		try {
			future = sm.asyncGrabGroupConfig();
			if (future == null) {
				exists.remove(sm);
				log.error("update all config server failed " + sm.getConfigServers() + " " + sm.getGroupName());
				return ;
			}
			final ResponseTask responseTask = new ResponseTask();
			responseTask.future 			= future;
			responseTask.request 			= request;
			request.updateCreateTime();
			                     
			future.setListener(new NkvFutureListener() {
				public void handle(Future<NkvRpcPacket> future) {
					tasks.offer(responseTask);
				}	
			});
		} catch (NkvRpcError e) {
			log.warn("update one config server failed ", e);
			tasks.offer(request);
		} catch (Exception e) {
			log.warn("some exception happens ", e);
			exists.remove(sm);
		}
	}
	
	private void doResponse(final ResponseTask responseTask) throws InterruptedException {
		GetGroupResponse response = null;
		try {
			response = responseTask.future.getResponse();
		} catch (InterruptedException e) {
			throw e;
		} catch (ExecutionException e) {
			responseTask.request.updateCreateTime();
			tasks.offer(responseTask.request);
			return;
		}
		try {
			responseTask.request.serverManager.update(response);
		} catch (Exception e) {
			log.error("update server list failed ", e);
		} finally {
			exists.remove(responseTask.request.serverManager);
		}
	}
	
	public void shutdown() {
		log.info("ConfigServer Updater shutdowning");
		this.interrupt();
		this.stop = false;
	}
}
