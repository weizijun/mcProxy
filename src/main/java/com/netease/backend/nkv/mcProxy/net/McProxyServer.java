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
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.impl.DefaultNkvClient;
import com.netease.backend.nkv.config.McProxyConfig;
import com.netease.backend.nkv.config.NkvConfig;

public class McProxyServer {
	private static final Logger logger = Logger.getLogger(McProxyServer.class);

	private McProxyConfig config;

	private ExecutorService bossThreadPool = null;
	private ExecutorService workerThreadPool = null;
	private NioServerSocketChannelFactory serverNioFactory = null;

	private ServerBootstrap bootstrap = null;
	
	private McRpcContext context = new McRpcContext();
	
	public McProxyServer(McProxyConfig config) {
		this.config = config;
	}

	public void init() {
		logger.info("mcProxy Server init, port = " + config.getPort());

		// init NioServerSocketChannelFactory
		bossThreadPool = Executors
				.newCachedThreadPool(new DeamondThreadFactory(
						"mcProxy-boss-share"));
		workerThreadPool = Executors
				.newCachedThreadPool(new DeamondThreadFactory(
						"mcProxy-worker-share"));
		serverNioFactory = new NioServerSocketChannelFactory(bossThreadPool,
				config.getBossThreadCount(), workerThreadPool,
				config.getWorkerThreadCount());

		// init ServerBootstrap
		bootstrap = new ServerBootstrap(serverNioFactory);
		final Timer timer = new HashedWheelTimer();
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("timeOutHandler", new ReadTimeoutHandler(timer, config.getKeepAliveTimeoutSeconds()));
				pipeline.addLast("LineDecoder", new LineBasedFrameDecoder(256));
				pipeline.addLast("handler", new McProxyHandler(context));
				return pipeline;
			}
		});
		bootstrap.setOption("tcpNoDelay", config.isTcpNoDelay());
		bootstrap.setOption("keepAlive", config.isKeepAlive());

		// bind port
		bootstrap.bind(new InetSocketAddress(config.getPort()));
	}

	public void destory() {
		logger.info("mcProxy Server destory");
		bootstrap.shutdown();
	}
	
	public static void main(String[] args) {
		McProxyConfig config = new McProxyConfig();
		config.setBossThreadCount(1);
		config.setWorkerThreadCount(4);
		config.setPort((short)11211);
		config.setKeepAlive(true);
		config.setTcpNoDelay(true);
		config.setKeepAliveTimeoutSeconds(60);
		
		McProxyServer server = new McProxyServer(config);
		server.init();
	}
}
