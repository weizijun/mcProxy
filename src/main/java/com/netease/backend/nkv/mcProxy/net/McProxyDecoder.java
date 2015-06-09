package com.netease.backend.nkv.mcProxy.net;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.netease.backend.nkv.client.error.McError;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.command.StorageCommand;
import com.netease.backend.nkv.mcProxy.command.text.TextDeleteCommand;
import com.netease.backend.nkv.mcProxy.command.text.TextGetCommand;
import com.netease.backend.nkv.mcProxy.command.text.TextSetCommand;

public class McProxyDecoder extends FrameDecoder{

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws McError {
		McProxyChannel mcProxyChannel = (McProxyChannel) channel.getAttachment();
		if (mcProxyChannel.getCacheParsingCommand() == null) {
			final int eol = findEndOfLine(buffer);
	        if (eol == -1) {
	        	return null;
	        }
	        
	        final int length = eol - buffer.readerIndex();
	        assert length >= 0: "Invalid length=" + length;
	        
	        int delimLength = 2;
	        byte[] cmdByteLine = new byte[length];
	        buffer.readBytes(cmdByteLine);
	        buffer.skipBytes(delimLength);
	        
	        Command cmd = null;
	        String cmdLine = new String(cmdByteLine);
	        String[] token = cmdLine.trim().split("\\s");
	        if (token[0].equals("get")) {
	        	cmd = new TextGetCommand();
	        } else if (token[0].equals("set")) {
	        	cmd = new TextSetCommand();
	        } else if (token[0].equals("delete")) {
	        	cmd = new TextDeleteCommand();
	        } else {
	        	throw new McError();
	        }
	  
	        cmd.decodeFrom(token);
	        
	        if (cmd instanceof StorageCommand) {
	        	mcProxyChannel.setCacheParsingCommand(cmd);
	        	return null;
	        } else {
	        	return cmd;
	        }
		} else {
			Command cmd =  mcProxyChannel.getCacheParsingCommand();
	        if (cmd instanceof StorageCommand) {
	        	StorageCommand storageCommand = (StorageCommand) cmd;
	        	boolean decodeResult = storageCommand.decodeValue(buffer);
	        	if (decodeResult == true) {
	        		mcProxyChannel.setCacheParsingCommand(null);
	        		return cmd;
	        	} else {
	        		return null;
	        	}
	        } else {
	        	throw new McError();
	        } 
		}
	}

    private int findEndOfLine(final ChannelBuffer buffer) {
        final int n = buffer.writerIndex();
        for (int i = buffer.readerIndex(); i < n; i ++) {
            final byte b = buffer.getByte(i);
            if (b == '\r' && i < n - 1 && buffer.getByte(i + 1) == '\n') {
                return i;  // \r\n
            }
        }
        return -1;  // Not found.
    }
}
