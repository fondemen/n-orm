package org.norm;

import java.util.Date;

import org.norm.Persisting;

@Persisting
public class Novel extends Book {

	public Novel(BookStore bookStore, Date sellerDate, Date receptionDate) {
		super(bookStore, sellerDate, receptionDate);
	}

}
