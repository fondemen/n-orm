package com.googlecode.n_orm.sample.businessmodel;

import java.util.Date;

import com.googlecode.n_orm.ImplicitActivation;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.hbase.HBaseSchema;
import com.googlecode.n_orm.hbase.HBaseSchema.SettableBoolean;


@Persisting
@HBaseSchema(inMemory=SettableBoolean.TRUE) //Annotation dedicated to HBase in order to overload store.properties ; merely ignored in case store is not HBase...
public class Book {
	private static final long serialVersionUID = -7771403847590578122L;
	@Key(order=1) @ImplicitActivation private BookStore bookStore; //ImplicitActivation makes the bookStore being activated/stored as soon as the book is activated/stored
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
		if (this.number <= 2) {
			this.bookStore.setToBeOrdered(this);
		}
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

	void setReceptionDate(Date receptionDate) {
		this.receptionDate = receptionDate;
	}
}
