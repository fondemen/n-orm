package com.googlecode.n_orm.mocked;

import com.googlecode.n_orm.FederatedMode;
import com.googlecode.n_orm.Persisting;

@Persisting(federated=FederatedMode.CONS, table="t")
public class ElementInFederatedMockedStore {
	private static final long serialVersionUID = -6681563816829930407L;
	public String key;
	public String post;
	
	public String getTablePostfix() {
		return this.post;
	}
}
