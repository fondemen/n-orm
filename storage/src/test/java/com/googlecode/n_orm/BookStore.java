package com.googlecode.n_orm;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class BookStore {
	private static final long serialVersionUID = -3919962605456785443L;
	@Key private String hashcode;
	private String name;

	public BookStore() {}
	
	public BookStore(String hashcode) {
		this.hashcode = hashcode;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getHashcode() {
		return hashcode;
	}

	protected void setHashcode(String hashcode) {
		this.hashcode = hashcode;
	}

}
