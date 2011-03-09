package org.norm.sample.businessmodel;

import com.mt.storage.Key;
import com.mt.storage.Persisting;

@Persisting
public class BookStore {
	private static final long serialVersionUID = -3919962605456785443L;
	@Key private String name;
	private String address;

	public BookStore() {}
	
	public BookStore(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

}
