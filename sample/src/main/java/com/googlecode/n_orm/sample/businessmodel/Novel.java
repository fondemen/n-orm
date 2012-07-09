package com.googlecode.n_orm.sample.businessmodel;

import java.util.Date;

import com.googlecode.n_orm.Persisting;


@Persisting
public class Novel extends Book {
	private static final long serialVersionUID = -1668891788907688227L;

	public Novel(BookStore bookStore, String title, Date receptionDate) {
		super(bookStore, title, receptionDate);
	}

}
