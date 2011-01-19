package com.mt.storage;

import java.util.Date;

@Persisting
public class Book {
	@Key(order=1) private final BookStore bookStore;
	@Key(order=2) private final Date sellerDate;
	@Key(order=3) private final Date receptionDate;
	private short number;
	
	public Book(com.mt.storage.BookStore bookStore, Date sellerDate, Date receptionDate) {
		this.bookStore = bookStore;
		this.sellerDate = sellerDate;
		this.receptionDate = receptionDate;
	}

	public short getNumber() {
		return number;
	}

	public void setNumber(short number) {
		this.number = number;
	}

	public BookStore getBookStore() {
		return bookStore;
	}

	public Date getSellerDate() {
		return sellerDate;
	}

	public Date getReceptionDate() {
		return receptionDate;
	}
}
