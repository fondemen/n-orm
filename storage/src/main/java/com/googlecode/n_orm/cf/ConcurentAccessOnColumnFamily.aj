package com.googlecode.n_orm.cf;

import java.util.ConcurrentModificationException;

public aspect ConcurentAccessOnColumnFamily {

	void around(ColumnFamily self, Object pojo): (call(void ColumnFamily.updateFromPOJO(Object)) || call(void ColumnFamily.storeToPOJO(Object))) && target(self) && args(pojo) {
		int maxRetries = 10;
		ConcurrentModificationException cme = null;
		do {
			try {
				synchronized (self.getOwner()) {
					synchronized(pojo) {
						proceed(self, pojo);
					}
				}
				cme = null;
			} catch (ConcurrentModificationException x) {
				if (cme == null) {
					System.err.println("Got a ConcurrentModificationException; please protect your changes to column families within a synchronized section. Retrying for the next 500 ms.");
					x.printStackTrace();
				}
				cme = x;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
			maxRetries--;
		} while (cme != null && maxRetries>=0);
		if (cme != null)
			throw cme;
		
	}
}
