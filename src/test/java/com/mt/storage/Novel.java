package com.mt.storage;

import java.util.Date;

@Persisting
public class Novel extends Book {

	public Novel(BookStore bookStore, Date sellerDate, Date receptionDate) {
		super(bookStore, sellerDate, receptionDate);
	}

}
