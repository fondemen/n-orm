package org.norm.sample.businessmodel;

import java.util.Date;

import org.norm.Persisting;


@Persisting
public class Novel extends Book {
	private static final long serialVersionUID = -1668891788907688227L;

	public Novel(BookStore bookStore, String title, Date receptionDate) {
		super(bookStore, title, receptionDate);
	}

}
