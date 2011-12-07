package com.googlecode.n_orm;

import java.util.Set;

import com.googlecode.n_orm.cf.ColumnFamily;

public class DefaultPersistingElementListener implements
		PersistingElementListener {

	@Override
	public void stored(PersistingElement pe) {
	}

	@Override
	public void storeInvoked(PersistingElement pe) {
	}

	@Override
	public void activated(PersistingElement pe,
			Set<ColumnFamily<?>> activatedColumnFamilies) {
	}

	@Override
	public void activateInvoked(PersistingElement pe,
			Set<ColumnFamily<?>> columnFamiliesToBeActivated) {
	}

}
