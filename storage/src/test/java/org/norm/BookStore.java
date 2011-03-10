package org.norm;

import org.norm.Key;
import org.norm.Persisting;

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

}
