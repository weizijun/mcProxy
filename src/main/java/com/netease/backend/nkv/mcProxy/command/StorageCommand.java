package com.netease.backend.nkv.mcProxy.command;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.error.McError;

/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午3:40:29
 */
public abstract class StorageCommand extends Command {
	public abstract boolean decodeValue(ChannelBuffer buffer) throws McError;
}
