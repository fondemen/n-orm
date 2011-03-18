package com.googlecode.n_orm;

import java.util.Date;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Book {
	private static final long serialVersionUID = -7771403847590578122L;
	@Key(order=1) private BookStore bookStore;
	@Key(order=2) private Date sellerDate;
	@Key(order=3) private Date receptionDate;
	private short number;
	
	public Book() {
		super();
	}
	
	public Book(com.googlecode.n_orm.BookStore bookStore, Date sellerDate, Date receptionDate) {
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

	public void setBookStore(BookStore bookStore) {
		this.bookStore = bookStore;
	}

	public void setSellerDate(Date sellerDate) {
		this.sellerDate = sellerDate;
	}

	public void setReceptionDate(Date receptionDate) {
		this.receptionDate = receptionDate;
	}
}
