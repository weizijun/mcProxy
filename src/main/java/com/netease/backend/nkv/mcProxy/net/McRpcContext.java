package com.netease.backend.nkv.mcProxy.net;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelStateEvent;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.impl.DefaultNkvClient;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.config.NkvConfig;

/**
 * @author hzweizijun 
 * @date 2015年6月1日 下午6:56:21
 */
public class McRpcContext {
	private static final Logger logger = Logger.getLogger(McRpcContext.class);
	
	private ConcurrentMap<SocketAddress, McProxyChannel> clientMap = new ConcurrentHashMap<SocketAddress, McProxyChannel>();
	
	public static volatile Object lock = new Object();
	
	private DefaultNkvClient nkvClient;
	private NkvOption opt = new NkvOption();
	private Short ns = 0;
	
	public McRpcContext() {
		NkvConfig nkvConfig = new NkvConfig();
		nkvConfig.setMaster("10.120.148.135:8200");
		nkvConfig.setSlave("10.120.148.135:8500");
		nkvConfig.setGroup("group_1");
		
		nkvClient = new DefaultNkvClient();
		nkvClient.setMaster(nkvConfig.getMaster());
		nkvClient.setSlave(nkvConfig.getSlave());
		nkvClient.setGroup(nkvConfig.getGroup());
		
		try {
			nkvClient.init();
		} catch (NkvException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void channelConnected(Channel channel)
			throws Exception {
		logger.info("client connect,client:" + channel.getRemoteAddress());
		McProxyChannel mcProxyChannel = new McProxyChannel(channel);
		channel.setAttachment(mcProxyChannel);
		clientMap.put(channel.getRemoteAddress(), mcProxyChannel);
	}
	
	public void messageReceived(Channel channel, ChannelBuffer buffer)
			throws Exception {
		logger.debug("McRpcContext messageReceived");
		int commandLen = buffer.readableBytes();
		if (commandLen <= 0) {
			return ;
		}

		byte[] command = new byte[commandLen];
		buffer.readBytes(command);
		String strCommand = new String(command);
		String[] cmdArray = strCommand.split("\\s+");
		if (cmdArray.length <= 1) {
			logger.error("message error:" + strCommand);
			throw new Exception("message error");
		}
		
		String cmdName = cmdArray[0];
		if(cmdName.equalsIgnoreCase("get") && cmdArray.length == 2) {
			McProxyChannel mcProxyChannel = (McProxyChannel) channel.getAttachment();
			
			String key = cmdArray[1];
			Future<Result<byte[]>> future = nkvClient.getAsync(ns, key.getBytes(), opt);
			@SuppressWarnings("unchecked")
			NkvResultFutureImpl<GetResponse, Result<byte[]>> realFuture = (NkvResultFutureImpl<GetResponse, Result<byte[]>>) future;
			realFuture.getImpl().setClientChannel((McProxyChannel) channel.getAttachment());
			Integer clientSeq = mcProxyChannel.putResult(realFuture.getImpl());
			realFuture.getImpl().setClientSeq(clientSeq);
		} else if (cmdName.equalsIgnoreCase("set") && cmdArray.length == 3) {
			String key = cmdArray[1];
			String value = cmdArray[2];
			Result<Void> r = nkvClient.put(ns, key.getBytes(), value.getBytes(), opt);
			System.out.println(r);
			String result = r.toString() + "\n";
			ChannelBuffer returnData = ChannelBuffers.buffer(result.length());
			returnData.writeBytes(result.getBytes());
			channel.write(returnData);
		} else if (cmdName.equalsIgnoreCase("remove") && cmdArray.length == 2){
			String key = cmdArray[1];
			Result<Void> r = nkvClient.invalidByProxy(ns, key.getBytes(), opt);
			System.out.println(r);
			String result = r.toString() + "\n";
			System.out.println(result);
			ChannelBuffer returnData = ChannelBuffers.buffer(result.length());
			returnData.writeBytes(result.getBytes());
			channel.write(returnData);
		} else {
			logger.error("message format error:" + strCommand);
			throw new Exception("message error");
		}
	}
	
	public void channelDisconnected(Channel channel, ChannelStateEvent e) {
		if (channel != null) {
			logger.info("client disconnect,clent:" + channel.getRemoteAddress());
			deleteSession(channel);		
		}
	}
	
	public void exceptionCaught(Channel channel, Throwable cause) {
		logger.info("client exceptionCaught,clent:" + channel.getRemoteAddress() + ",cause:" + cause );
		deleteSession(channel);
	}
	
	private void deleteSession(Channel channel) {
		McProxyChannel nettyChannel = clientMap.get(channel.getRemoteAddress());
		if (nettyChannel != null) {
			clientMap.remove(channel.getRemoteAddress());
			
			try {
				channel.close();
			} catch (Exception e) {
				logger.warn("close channel exception " + channel, e);
			}
		} else {
			logger.warn("delete session with: null channel, connection: "
					+ channel.getRemoteAddress());
		}
	}
}
