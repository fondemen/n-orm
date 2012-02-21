package com.googlecode.n_orm;

import java.util.Date;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class RealBook {
	private static final long serialVersionUID = -7771403847590578122L;
	@Key(order=1) private BookStore bookStore;
	@Key(order=2) private Date sellerDate;
	@Key(order=3) private Date receptionDate;
	@Key(order=4) private String name;
	@Key(order=5) private Integer serialID;

	private short number;
	
	public RealBook() {
		super();
	}

	public RealBook(BookStore bookStore, Date sellerDate, Date receptionDate,
			String name, Integer serialID) {
		super();
		this.bookStore = bookStore;
		this.sellerDate = sellerDate;
		this.receptionDate = receptionDate;
		this.name = name;
		this.serialID = serialID;
	}

	public BookStore getBookStore() {
		return bookStore;
	}

	public void setBookStore(BookStore bookStore) {
		this.bookStore = bookStore;
	}

	public Date getSellerDate() {
		return sellerDate;
	}

	public void setSellerDate(Date sellerDate) {
		this.sellerDate = sellerDate;
	}

	public Date getReceptionDate() {
		return receptionDate;
	}

	public void setReceptionDate(Date receptionDate) {
		this.receptionDate = receptionDate;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getSerialID() {
		return serialID;
	}

	public void setSerialID(Integer serialID) {
		this.serialID = serialID;
	}

	public short getNumber() {
		return number;
	}

	public void setNumber(short number) {
		this.number = number;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	

}
