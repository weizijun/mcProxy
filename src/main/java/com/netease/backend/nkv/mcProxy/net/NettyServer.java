package com.netease.backend.nkv.mcProxy.net;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.netease.backend.nkv.client.rpc.net.NettyConfig;

public class NettyServer {
	private static final Logger logger = Logger.getLogger(NettyServer.class);

	private NettyConfig nettyConfig;

	private ExecutorService bossThreadPool = null;
	private ExecutorService workerThreadPool = null;
	private NioServerSocketChannelFactory serverNioFactory = null;

	private ServerBootstrap bootstrap = null;
	
	private McRpcContext context = new McRpcContext();

	public void init() {
		logger.info("nettyService init, port = " + nettyConfig.getPort());

		// init NioServerSocketChannelFactory
		bossThreadPool = Executors
				.newCachedThreadPool(new DeamondThreadFactory(
						"redis-cloud-boss-share"));
		workerThreadPool = Executors
				.newCachedThreadPool(new DeamondThreadFactory(
						"redis-cloud-worker-share"));
		serverNioFactory = new NioServerSocketChannelFactory(bossThreadPool,
				nettyConfig.getBossThreadCount(), workerThreadPool,
				nettyConfig.getWorkerThreadCount());

		// init ServerBootstrap
		bootstrap = new ServerBootstrap(serverNioFactory);
		final Timer timer = new HashedWheelTimer();
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("timeOutHandler", new ReadTimeoutHandler(timer, nettyConfig.getKeepAliveTimeoutSeconds()));
				pipeline.addLast("handler", new NettyHandler(context));
				return pipeline;
			}
		});
		bootstrap.setOption("tcpNoDelay", nettyConfig.isTcpNoDelay());
		bootstrap.setOption("keepAlive", nettyConfig.isKeepAlive());

		// bind port
		bootstrap.bind(new InetSocketAddress(nettyConfig.getPort()));
	}

	public void destory() {
		logger.info("nettyService destory");
		bootstrap.shutdown();
	}
}
