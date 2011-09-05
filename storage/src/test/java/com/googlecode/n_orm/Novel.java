package com.googlecode.n_orm;

import java.util.Date;

import com.googlecode.n_orm.Persisting;

@Persisting
public class Novel extends Book {

	public Novel(BookStore bookStore, Date sellerDate, Date receptionDate) {
		super(bookStore, sellerDate, receptionDate);
	}

}
