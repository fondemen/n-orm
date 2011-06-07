package com.googlecode.n_orm.sample.businessmodel;

import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.StorageManagement;

@Persisting
public class BookStore {
	//A persisting element is serializable, thus needs a serialVersionUID
	private static final long serialVersionUID = -3919962605456785443L;
	
	//A key: not to be stored as a property, but an unique identifier for this element
	//Values for keys appear as row index in the data store
	@Key private String name;
	private String address;
	
	//The following is a column family
	private Set<Book> toBeOrdered = new TreeSet<Book>();

	//Necessary for serialization capabilities
	public BookStore() {}
	
	public BookStore(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	private void checkBook(Book book) {
		if (!book.getBookStore().equals(this))
			throw new IllegalArgumentException(book.toString() + " is not in " + this.toString());
	}
	
	//Finding books directly from the data store.
	//No need for a column family as Book has BookStore as a key
	public Set<Book> getBooks() {
		return StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(this).withAtMost(1000).elements().go();
	}
	
	//Creating a new book in the store
	public Book newBooks(String title, short number) {
		Book b = StorageManagement.findElements().ofClass(Book.class).withKey("bookStore").setTo(this)
					.andWithKey("title").setTo(title).any();
		if (b == null) {
			b = new Book(this, title, new Date());
		} else {
			b.activateIfNotAlready();
		}
		b.setNumber((short) (b.getNumber()+number));
		b.store();
		return b;
	}
	
	//A method that sends its data immediately to the data store
	public void sold(Book book) {
		checkBook(book);
		book.setNumber((short) (book.getNumber()-1));
		book.store();
	}
	
	void setToBeOrdered(Book book) {
		this.checkBook(book);
		this.toBeOrdered.add(book);
		this.store();
	}
	
	public Set<Book> getBooksToBeOrdered() {
		this.activate("toBeOrdered");
		return this.toBeOrdered;
	}

}
