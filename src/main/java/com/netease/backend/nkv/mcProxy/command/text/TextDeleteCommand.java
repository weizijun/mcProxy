package com.netease.backend.nkv.mcProxy.command.text;

import net.rubyeye.xmemcached.command.CommandType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.McClientError;
import com.netease.backend.nkv.client.error.McError;
import com.netease.backend.nkv.client.error.McServerError;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.command.ProtocolType;

/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午4:30:57
 */
public class TextDeleteCommand extends Command {
	
	public TextDeleteCommand() {
		protocolType = ProtocolType.Text;
		commandType = CommandType.DELETE;
	}

	@Override
	public <T> ChannelBuffer encodeTo(Result<T> result) throws McError {
		if (result.getCode() == ResultCode.OK) {
			return ChannelBuffers.copiedBuffer(DELETED);
		} else {
			throw new McServerError();
		}
	}

	@Override
	public void decodeFrom(String[] tokens) throws McError {
		if (tokens.length < 2) {
			throw new McError();
		}
		
		key = tokens[1];
		if (tokens.length == 3) {
			if (tokens[2].equals(NOREPLAY) == false) {
				throw new McClientError("bad command line format.  Usage: delete <key> [noreply]");
			} else {
				noreply = true;
			}
		} else if (tokens.length > 3){
			throw new McClientError("bad command line format.  Usage: delete <key> [noreply]");
		}
	}

}
