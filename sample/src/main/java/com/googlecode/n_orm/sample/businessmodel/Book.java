package com.googlecode.n_orm.sample.businessmodel;

import java.util.Date;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;


@Persisting
public class Book {
	private static final long serialVersionUID = -7771403847590578122L;
	@Key(order=1) private BookStore bookStore;
	@Key(order=2) private String title;
	@Key(order=3) private Date receptionDate;
	private short number;
	
	public Book() {
		super();
	}
	
	public Book(BookStore bookStore, String title, Date receptionDate) {
		this.bookStore = bookStore;
		this.title = title;
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

	public String getTitle() {
		return title;
	}

	public Date getReceptionDate() {
		return receptionDate;
	}
}
