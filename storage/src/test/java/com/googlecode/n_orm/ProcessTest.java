package com.googlecode.n_orm;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.n_orm.StorageManagement.ProcessReport;

public class ProcessTest {

	private BookStore bssut = new BookStore("testbookstore");
	private Novel n1, n2;
	
	@BeforeClass
	public static void deleteExistingNovel() {
		 for(Novel n : StorageManagement.findElements().ofClass(Novel.class).withAtMost(10000).elements().go())
			 n.delete();
	}
	
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
		
		private int inc = 1;
		private boolean activate = true;
		private boolean wait = false;
		
		public InrementNovel() {
		}
		
		public InrementNovel(boolean activate, boolean wait) {
			this.activate = activate;
			this.wait = wait;
		}

		public InrementNovel(int inc) {
			super();
			this.inc = inc;
		}
		
		@Override
		public void process(Novel element) throws InterruptedException {
			if (this.activate)
				element.activate();
			element.attribute+=inc;
			element.store();
			if (this.wait)
				Thread.sleep(20);
		}
	}
	 
	 @Test public void process() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 long start = System.currentTimeMillis();
		 ProcessReport<Novel> ret = StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(), 2, 20000);		 
		 long end = System.currentTimeMillis();
		 n1.activate();
		 n2.activate();
		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
		 assertEquals(2, ret.getElementsTreated());
		 assertEquals(n2, ret.getLastProcessedElement());
		 assertEquals(end-start, ret.getDurationInMillis(), 10);
	 }
	 
	 @Test public void processActivating() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().andActivate().forEach(new InrementNovel(false, false), 2, 20000);		 
		 n1.activate();
		 n2.activate();
		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test public void processNotActivating() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(false, false), 2, 20000);		 
		 n1.activate();
		 n2.activate();
		 assertEquals(1, n1.attribute);
		 assertEquals(1, n2.attribute);
	 }
	 
	 @Test public void processOneThread() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(false, false), 1, 20000);		 

		 //No need for activation as elements are processed in this thread
		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test public void processOneThreadDefaultParameters() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(false, false));		 

		 //No need for activation as elements are processed in this thread
		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test public void processNotActivatingOneThread() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 
		 StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(false, false), 1, 20000);		 

		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test(expected=ProcessException.class) public void processTooShort() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(true, true), 2, 1);		 
	 }
	 
	 @Test public void processTooShortOneThread() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		 assertEquals(1, n1.attribute);
		 assertEquals(2, n2.attribute);
		 
		StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(false, false), 1, 1);				 

		 //No need for activation as elements are processed in this thread
		 assertEquals(2, n1.attribute);
		 assertEquals(3, n2.attribute);
	 }
	 
	 @Test public void processUntimeouted() throws DatabaseNotReachedException, InterruptedException, ProcessException {
		StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new InrementNovel(), 1, Long.MAX_VALUE);		 
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
				synchronized(this) {
					if (isRunning)
						throw new Exception();
					isRunning = true;
				}
				Thread.sleep(50);
				synchronized(this) {
					isRunning = false;
				}
			}
		};
		 @Test(expected=ProcessException.class) public void tooMuchProcesses() throws DatabaseNotReachedException, InterruptedException, ProcessException {
			StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new ThreadUnsafeProcess(), 3, 20000);	
		 }
		 @Test public void onlyOneProcess() throws DatabaseNotReachedException, InterruptedException, ProcessException {
			StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new ThreadUnsafeProcess(), 1, 20000);	
		 }
		 
		 @Test public void processWithExecutor() throws DatabaseNotReachedException, InterruptedException, ProcessException {
			ExecutorService executor = Executors.newFixedThreadPool(1);
			StorageManagement.findElements().ofClass(Novel.class).withAtMost(1000).elements().forEach(new ThreadUnsafeProcess(), 3, 20000, executor);
			assertFalse(executor.isTerminated());
			executor.shutdown();
			assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
		 }

}
