package com.mt.storage;

@Persisting
public class BookStore {
	@Key private final String hashcode;
	private String name;

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
