package org.norm;

import java.util.Date;

import org.norm.Key;
import org.norm.Persisting;

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
	
	public Book(org.norm.BookStore bookStore, Date sellerDate, Date receptionDate) {
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
