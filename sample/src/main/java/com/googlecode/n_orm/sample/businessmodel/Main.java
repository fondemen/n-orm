package com.googlecode.n_orm.sample.businessmodel;

import java.util.Date;

import com.googlecode.n_orm.StorageManagement;

public class Main {

	public static void main(String[] args) {
		
		BookStore obs = new BookStore("cnaf");
		obs.setAddress("Turing str. 41");
		obs.store();
		String id = obs.getIdentifier();
		
		BookStore bs = StorageManagement.getElement(BookStore.class, id);
		assert obs.equals(bs);
		
		bs = StorageManagement.getElementWithKeys(BookStore.class, "cnaf");
		assert obs.equals(bs);
		
		assert 0 == StorageManagement.findElements().ofClass(Book.class)
		        .withKey("bookStore").setTo(obs).count();
		
		Book b = new Book(obs, "n-orm for dummies", new Date());
		b.setReceptionDate(new Date());
		b.setNumber((short) 10);
		b.store();
		
		assert 1 == StorageManagement.findElements().ofClass(Book.class)
		        .withKey("bookStore").setTo(obs).count();

		System.out.println("=========================");
		System.out.println("It seems that it works !");
		System.out.println("=========================");
	}

}
