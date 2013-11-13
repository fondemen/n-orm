package com.googlecode.n_orm.sample.businessmodel;

import java.util.Date;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.WaitingCallBack;

public class Main {

	public static void main(String[] args) throws Throwable {

		BookStore obs = new BookStore("cnaf");
		obs.setAddress("Turing str. 41");
		obs.store();
		String id = obs.getIdentifier();

		BookStore bs = StorageManagement.getElement(BookStore.class, id);
		assert obs.equals(bs);

		bs = StorageManagement.getElementWithKeys(BookStore.class, "cnaf");
		assert obs.equals(bs);

		StorageManagement.findElements().ofClass(Book.class)
				.withKey("bookStore").setTo(bs)
				.withAtMost(Integer.MAX_VALUE).elements()
				.forEach(new Process<Book>() {

					@Override
					public void process(Book element) throws Throwable {
						element.delete();
					}
				});

		assert 0 == StorageManagement.findElements().ofClass(Book.class)
				.withKey("bookStore").setTo(obs).count();

		Book b = new Book(obs, "n-orm for dummies", new Date());
		b.setReceptionDate(new Date());
		b.setNumber((short) 10);
		b.store();

		assert 1 == StorageManagement.findElements().ofClass(Book.class)
				.withKey("bookStore").setTo(obs).count();

		WaitingCallBack wc = new WaitingCallBack();
		StorageManagement.findElements().ofClass(Book.class)
				.withKey("bookStore").setTo(b.getBookStore()).andActivate()
				.withAtMost(Integer.MAX_VALUE).elements()
				.remoteForEach(new Process<Book>() {

					@Override
					public void process(Book element) throws Throwable {
						element.delete();
						System.out.println("Deleted created book " + element
								+ " with map/reduce");
					}
				}, wc, 1, 10000);
		wc.waitProcessCompleted();
		if (wc.getError() != null)
			throw wc.getError();

		assert 0 == StorageManagement.findElements().ofClass(Book.class)
				.withKey("bookStore").setTo(obs).count();

		System.out.println("=========================");
		System.out.println("It seems that it works !");
		System.out.println("=========================");
	}
}
