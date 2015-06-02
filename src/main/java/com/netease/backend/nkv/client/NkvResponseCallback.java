package com.netease.backend.nkv.client;

/**
 * Callback
 * @author tianmai.fh 
 * @date 2013-4-3
 */
public interface NkvResponseCallback {
   
   public void callback(Result<?> resp) ;
   
   public void callback(Throwable e);
   
}
