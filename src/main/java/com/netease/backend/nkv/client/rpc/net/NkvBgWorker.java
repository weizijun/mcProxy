package com.netease.backend.nkv.client.rpc.net;

import java.util.ArrayList;
import java.util.concurrent.DelayQueue;

public class NkvBgWorker extends Thread {
	
	/*public static class WaitingChannelSeq implements Delayed {
		
		private NkvChannel session;
		private Integer chid;
		private long delayed;
		
		public WaitingChannelSeq(NkvChannel session, Integer chid, long timeout) {
			this.session = session;
			this.chid = chid;
			delayed = timeout + System.currentTimeMillis();
		}

		public int compareTo(Delayed o) {
			WaitingChannelSeq obj = (WaitingChannelSeq)o;
			long r = this.delayed - obj.delayed;
			if (r < 0) 
				return -1;
			else if (r == 0)
				return 0;
			else
				return 1;
		}

		public long getDelay(TimeUnit unit) {
			return unit.convert(delayed - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
			//return delayed - System.currentTimeMillis();
		}
		
	}*/
	
	public NkvBgWorker() {
		this.setDaemon(true);
		this.setName("Nkv-Timeout-Channel-Checker");
	}
	
	protected DelayQueue<WaitingChannelSeq> queue = new DelayQueue<WaitingChannelSeq>();

	@Override
	public void run() {
		while (!Thread.interrupted()) {
			try {
				
				ArrayList<WaitingChannelSeq> wchids = new ArrayList<WaitingChannelSeq>();
				wchids.add(queue.take());
				queue.drainTo(wchids);
				for (WaitingChannelSeq wc : wchids) {
					wc.session.clearTimeoutCallTask(wc.chid);
				}
				Thread.sleep(5);
			} catch (InterruptedException e) {
				break;
			}
		}
	}
	
	public boolean addWaitingChannelSeq(WaitingChannelSeq entry) {
		return queue.add(entry);
	}
	
	public boolean removeWaitingChannelSeq(WaitingChannelSeq entry) {
		return queue.remove(entry);
	}

	public boolean addWaitingChannelSeq(NkvChannel session, Integer i, long timeout) {
		WaitingChannelSeq waitingChannelSeq = new WaitingChannelSeq(session, i, timeout);
		session.SetWaitingChannelSeq(waitingChannelSeq);
		return queue.add(waitingChannelSeq);
	}
}
