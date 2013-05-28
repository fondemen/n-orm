package com.googlecode.n_orm;

import java.util.Date;

import com.googlecode.n_orm.Persisting;

@Persisting
public class Novel extends Book {
	private static final long serialVersionUID = 4054715604773930022L;
	public int attribute;

	public Novel(BookStore bookStore, Date sellerDate, Date receptionDate) {
		super(bookStore, sellerDate, receptionDate);
	}

}
