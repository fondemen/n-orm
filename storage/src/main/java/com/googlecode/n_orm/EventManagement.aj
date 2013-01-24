package com.googlecode.n_orm;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.storeapi.Store;


public aspect EventManagement {

	private transient boolean PersistingElement.isStored = false;
	
	private transient Collection<PersistingElementListener> PersistingElement.listeners;
	
	public boolean PersistingElement.hasListener(PersistingElementListener listener) {
		if (this.listeners == null)
			return false;
		return this.listeners.contains(listener);
	}
	
	public void PersistingElement.addPersistingElementListener(PersistingElementListener listener) {
		if (this.listeners == null)
			this.listeners = new LinkedList<PersistingElementListener>();
		if (this.hasListener(listener))
			return;
		this.listeners.add(listener);
	}
	
	public void PersistingElement.removePersistingElementListener(PersistingElementListener listener) {
		if (this.hasListener(listener))
			this.listeners.remove(listener);
	}
	
	after (PersistingElement self, boolean storing): set(boolean PersistingElement+.isStoring) && withincode(void PersistingElement+.store()) && target(self) && args(storing) && if(storing)  {
		if (self.listeners != null)
			for (PersistingElementListener listener : self.listeners) {
					listener.storeInvoked(self);
			}
	}
	
	after (PersistingElement self) returning: execution(void PersistingElement+.store()) && target(self)  {
		if (self.isStored) {
			self.isStored = false;
			if (self.listeners != null)
				for (PersistingElementListener listener : self.listeners) {
						listener.stored(self);
				}
		}
	}
	
	after (PersistingElement self) returning: call(void Store+.storeChanges(..)) && withincode(void PersistingElement+.store()) && this(self)  {
		self.isStored = true;
	}
	
	before (PersistingElement self, String[] families) : execution(private void PersistingElement.activate(long, String...)) && this(self) && args(long, families) {
		if (self.listeners != null) {
			Set<ColumnFamily<?>> fams = new TreeSet<ColumnFamily<?>>();
			for (String famName : families) {
				fams.add(self.getColumnFamily(famName));
			}
			for (PersistingElementListener listener : self.listeners) {
					listener.activateInvoked(self, fams);
			}
		}
	}
	
	after (PersistingElement self, Set<String> activated) returning: execution(void PersistingElement+.activateFromRawData(Set<String>, *)) && this(self) && args(activated, ..) {
		if (self.listeners != null) {
			Set<ColumnFamily<?>> fams = new TreeSet<ColumnFamily<?>>();
			for (String famName : activated) {
				fams.add(self.getColumnFamily(famName));
			}
			for (PersistingElementListener listener : self.listeners) {
					listener.activated(self, fams);
			}
		}
	}
}
