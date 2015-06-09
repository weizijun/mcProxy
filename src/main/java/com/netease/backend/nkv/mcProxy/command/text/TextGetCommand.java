package com.netease.backend.nkv.mcProxy.command.text;

import net.rubyeye.xmemcached.command.CommandType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.McError;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.command.ProtocolType;

/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午2:32:15
 */
public class TextGetCommand extends Command {
	
	public TextGetCommand() {
		protocolType = ProtocolType.Text;
		commandType = CommandType.GET_ONE;
	}

	@Override
	public <T> ChannelBuffer encodeTo(Result<T> result) throws McError {
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		@SuppressWarnings("unchecked")
		Result<byte[]> getResult = (Result<byte[]>) result;
		if (result.getCode() == ResultCode.OK) {
			buffer.writeBytes(getResult.getResult());
			buffer.writeBytes(ENDBYTES);
		}

		buffer.writeBytes(END);
		return buffer;
	}

	@Override
	public void decodeFrom(String[] tokens) throws McError {
		assert tokens.length == 2 : "Invalid format";
		key = tokens[1];
	}

}
