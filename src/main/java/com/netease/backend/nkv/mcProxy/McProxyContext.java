package com.netease.backend.nkv.mcProxy;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.rubyeye.xmemcached.command.CommandType;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelStateEvent;

import com.netease.backend.nkv.client.NkvClient.NkvOption;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.error.McError;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.impl.DefaultNkvClient;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureSetImpl;
import com.netease.backend.nkv.config.McProxyConfig;
import com.netease.backend.nkv.config.NkvConfig;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.net.McProxyChannel;
import com.netease.backend.nkv.mcProxy.net.McProxyServer;

/**
 * @author hzweizijun 
 * @date 2015年6月1日 下午6:56:21
 */
public class McProxyContext {
	private static final Logger logger = Logger.getLogger(McProxyContext.class);
	
	private ConcurrentMap<SocketAddress, McProxyChannel> clientMap = new ConcurrentHashMap<SocketAddress, McProxyChannel>();
	
	public static volatile Object lock = new Object();
	
	private DefaultNkvClient nkvClient;
	private NkvOption opt = new NkvOption();
	private Short ns = 0;
	
	public McProxyContext() {
		NkvConfig nkvConfig = new NkvConfig();
		nkvConfig.setMaster("10.120.148.135:8200");
		nkvConfig.setSlave("10.120.148.135:8500");
		nkvConfig.setGroup("group_1");
		
		nkvClient = new DefaultNkvClient();
		nkvClient.setMaster(nkvConfig.getMaster());
		nkvClient.setSlave(nkvConfig.getSlave());
		nkvClient.setGroup(nkvConfig.getGroup());
		
		opt.setTimeout(100000000);
		
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
	
	public void messageReceived(Channel channel, Command command)
			throws Exception {
		ChannelBuffer buffer = ChannelBuffers.copiedBuffer(command.getKey());
		channel.write(buffer);
		
		McProxyChannel mcProxyChannel = (McProxyChannel) channel.getAttachment();
		
		if (command.getCommandType() == CommandType.GET_ONE) {
			NkvResultFutureImpl<GetResponse, Result<byte[]>> future = nkvClient.getAsync(ns, command.getKey(), opt);
			QueryMsg msg = new QueryOneMsg(future, mcProxyChannel, command);
			mcProxyChannel.offerMsg(msg);
			future.getImpl().setQueryMsg(msg);
		} else if (command.getCommandType() == CommandType.SET) {
			NkvResultFutureImpl<ReturnResponse, Result<Void>> future = nkvClient.putAsync(ns, command.getKey(), command.getValue(), opt);
			QueryMsg msg = new QueryOneMsg(future, mcProxyChannel, command);
			mcProxyChannel.offerMsg(msg);
			future.getImpl().setQueryMsg(msg);
		} else if (command.getCommandType() == CommandType.DELETE) {
			Result<Void> r = nkvClient.invalidByProxy(ns, command.getKey(), opt);
			System.out.println(r);
			String result = r.toString() + "\n";
			System.out.println(result);
			ChannelBuffer returnData = ChannelBuffers.buffer(result.length());
			returnData.writeBytes(result.getBytes());
			channel.write(returnData);
		} else if (command.getCommandType() == CommandType.GET_MANY) {
			NkvResultFutureSetImpl<GetResponse, byte[], ResultMap<String, Result<byte[]>>> futureSet = nkvClient.batchGetAsync(ns, command.getKeys(), opt);
			QueryMsg msg = new QueryMultiMsg(futureSet, mcProxyChannel, command);
			mcProxyChannel.offerMsg(msg);
			futureSet.setFutureQueryMsg(msg);
		} else {
			logger.error("message format error:" + command.getCommandType());
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
		
		if (cause instanceof McError) {
			try {
				String errorMsg = cause.getMessage();
				ChannelBuffer errorBuffer = ChannelBuffers.buffer(errorMsg.length());
				errorBuffer.writeBytes(errorMsg.getBytes());
				channel.write(errorBuffer);
			} catch (Exception e) {
				logger.error("", e);
			} finally {
				McProxyChannel mcProxyChannel = (McProxyChannel) channel.getAttachment();
				mcProxyChannel.setCacheParsingCommand(null);
			}
		} else {
			deleteSession(channel);
		}
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
	
	public static void main(String[] args) throws InterruptedException {
		McProxyConfig config = new McProxyConfig();
		config.setBossThreadCount(1);
		config.setWorkerThreadCount(4);
		config.setPort((short)11211);
		config.setKeepAlive(true);
		config.setTcpNoDelay(true);
		config.setKeepAliveTimeoutSeconds(60);
		
		McProxyServer server = new McProxyServer(config);
		server.init();
		while (true) {
			Thread.sleep(100000);
		}
	}
}
