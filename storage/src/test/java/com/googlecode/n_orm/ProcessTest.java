package com.googlecode.n_orm;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProcessTest {

	private BookStore bssut = new BookStore("testbookstore");
	private Novel n1, n2;
	
	@Before public void createNovels() {
		 n1 = new Novel(bssut, new Date(123456799), new Date(0));
		 n1.attribute = 1;
		 n1.store();
		 n2 = new Novel(bssut, new Date(123456799), new Date(1));
		 n2.attribute = 2;
		 n2.store();
	}
	
	@After public void deleteNovels() {
		n1.delete();
		n2.delete();
	}
	
	public static class InrementNovel implements Process<Novel> {
		private static final long serialVersionUID = 4763391618006514705L;
		
		private final int inc;
		
		public InrementNovel() {
			inc = 1;
		}

		public InrementNovel(int inc) {
			super();
			this.inc = inc;
		}

		@Override
		public void process(Novel element) {
			//element.activate();
			element.attribute+=inc;
			element.store();
		}
	}
	 
	 @Test public void process() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(), 2, 20000);		 

		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test public void processOneThread() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(), 1, 20000);		 

		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test(expected=InterruptedException.class) public void processTooShort() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(), 1, 1);		 
	 }

		
		public static class BadProcess implements Process<Novel> {
			private static final long serialVersionUID = -3453815599293767062L;

			@Override
			public void process(Novel element) throws Throwable {
				throw new Exception();
			}
		};
		 @Test(expected=ProcessException.class) public void badProcess() throws DatabaseNotReachedException, InterruptedException, ProcessException {
			 try {
				 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new BadProcess(), 2, 20000);	
			 } catch (ProcessException x) {
				 assertEquals(StorageManagement.findElements().ofClass(Novel.class).count(), x.getProblems().size());
				 throw x;
			 }
		 }
			
		public static class ThreadUnsafeProcess implements Process<Novel> {
			private static final long serialVersionUID = -3453815599293767062L;
			
			private volatile boolean isRunning = false;

			@Override
			public void process(Novel element) throws Throwable {
				if (isRunning)
					throw new Exception();
				isRunning = true;
				Thread.sleep(10);
				isRunning = false;
			}
		};
		 @Test(expected=ProcessException.class) public void tooMuchProcesses() throws DatabaseNotReachedException, InterruptedException, ProcessException {
			StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new ThreadUnsafeProcess(), 3, 20000);	
		 }
		 @Test public void onlyOneProcess() throws DatabaseNotReachedException, InterruptedException, ProcessException {
			StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new ThreadUnsafeProcess(), 1, 20000);	
		 }

}
