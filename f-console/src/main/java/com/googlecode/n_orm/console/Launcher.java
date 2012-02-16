package com.googlecode.n_orm.console;

import java.io.IOException;
import java.util.Date;
import com.googlecode.n_orm.StorageManagement;
import com.googlecode.n_orm.console.shell.Shell;
import com.googlecode.n_orm.sample.businessmodel.Book;
import com.googlecode.n_orm.sample.businessmodel.BookStore;

public class Launcher
{
	public static void main(String[] args) throws IOException
	{
		initMemoryBase();
		
		Shell shell = new Shell();
		shell.putEntryMapCommand(StorageManagement.class.getName(), new StorageManagement());
		shell.launch();
	}
	
	private static void initMemoryBase()
	{
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
//		b.setReceptionDate(new Date());
		b.setNumber((short) 10);
		b.store();
		
		assert 1 == StorageManagement.findElements().ofClass(Book.class)
		        .withKey("bookStore").setTo(obs).count();
	}
}