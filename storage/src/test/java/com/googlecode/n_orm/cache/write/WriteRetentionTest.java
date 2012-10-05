package com.googlecode.n_orm.cache.write;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;
import com.googlecode.n_orm.storeapi.Store;

public class WriteRetentionTest {

	private static final String table = "testtable";
	private static final String rowId = "testrow";
	private static final String changedCf = "changedCf";
	private static final String incrementedCf = "incrementedCf";
	private static final String changedKey = "changedKey";
	private static final String incrementedKey = "incrementedKey";
	private static final byte[] changedValue1 = ConversionTools.convert("changedValue1");
	private static final byte[] changedValue2 = ConversionTools.convert("changedValue2");
	private static ColumnFamilyData aChange, anotherChange;
	private static Map<String, Set<String>> aDelete;
	private static Map<String, Map<String, Number>> anIncrement;
	private static WriteRetentionStore sut50;
	private static WriteRetentionStore sut200;
	private static WriteRetentionStore sut50Mock;
	private static Store store = SimpleStoreWrapper.getWrapper(Memory.INSTANCE);
	private static Store mockStore = Mockito.mock(Store.class);
	
	@BeforeClass
	public static void setupPossibleSuts() {
		sut50Mock = WriteRetentionStore.getWriteRetentionStore(1, mockStore);
		sut50Mock.start();
		sut50 = WriteRetentionStore.getWriteRetentionStore(50, store);
		sut50.start();
		sut200 = WriteRetentionStore.getWriteRetentionStore(200, store);
		sut200.start();
		
		aChange = new DefaultColumnFamilyData();
		Map<String, byte[]> changedValues = new TreeMap<String, byte[]>();
		changedValues.put(changedKey, changedValue1);
		aChange.put(changedCf, changedValues );
		
		anotherChange = new DefaultColumnFamilyData();
		changedValues = new TreeMap<String, byte[]>();
		changedValues.put(changedKey, changedValue2);
		anotherChange.put(changedCf, changedValues );
		
		aDelete = new TreeMap<String, Set<String>>();
		Set<String> deleted = new TreeSet<String>();
		deleted.add(changedKey);
		aDelete.put(changedCf, deleted);
		
		anIncrement = new TreeMap<String, Map<String,Number>>();
		Map<String, Number> incr = new TreeMap<String, Number>();
		incr.put(incrementedKey, 1);
		anIncrement.put(incrementedCf, incr );
	}
	
	@Before
	public void cleanupStore() {
		Memory.INSTANCE.reset();
	}
	
	@Before
	public void resetMock() {
		Mockito.reset(mockStore);
	}
	
	@After
	public void waitForPendingRequests() {
		for (WriteRetentionStore wrs : new WriteRetentionStore[] {sut50Mock, sut50, sut200}) {
			while(wrs.getPendingRequests() != 0)
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	
	@Test
	public void getAlwaysSameRetensionStore() throws InterruptedException, ExecutionException {
		final int parallelGets = 100;
		final Object wait = new Object();
		final Store s = Mockito.mock(Store.class);
		final CountDownLatch waitForAllTasksToBeStarted = new CountDownLatch(parallelGets);
		Callable<WriteRetentionStore> getWRS = new Callable<WriteRetentionStore>() {

			@Override
			public WriteRetentionStore call() throws Exception {
				waitForAllTasksToBeStarted.countDown();
				synchronized(wait) {
					wait.wait();
				}
				return WriteRetentionStore.getWriteRetentionStore(1234, s);
			}
		};
		ExecutorService es = Executors.newCachedThreadPool();
		Collection<Future<WriteRetentionStore>> results = new LinkedList<Future<WriteRetentionStore>>();
		for (int i = 0; i < parallelGets; i++) {
			results.add(es.submit(getWRS));
		}
		waitForAllTasksToBeStarted.await();
		synchronized(wait) {
			wait.notifyAll();
		}
		es.shutdown();
		WriteRetentionStore found = null;
		for (Future<WriteRetentionStore> res : results) {
			if (found == null) {
				found = res.get();
				assertNotNull(found);
			} else {
				assertSame(found, res.get());
			}
		}
	}
	
	@Test
	public void retensionChange() throws InterruptedException {
		WriteRetentionStore sut = sut200;
		
		sut.storeChanges(null, table, rowId, aChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(100);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(150);
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test
	public void twoChanges() throws InterruptedException {
		WriteRetentionStore sut = sut50;
		sut.storeChanges(null, table, rowId, aChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		sut.storeChanges(null, table, rowId, anotherChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no store
		Thread.sleep(100);
		assertTrue(Memory.INSTANCE.hadAQuery()); // the store query
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test
	public void delete() throws InterruptedException {
		WriteRetentionStore sut = sut50;
		Memory.INSTANCE.storeChanges(table, rowId, aChange, null, null);
		Memory.INSTANCE.resetQueries();
		
		sut.delete(null, table, rowId);
		assertTrue(store.exists(null, table, rowId));
		Thread.sleep(10);
		assertTrue(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no delete
		Thread.sleep(90);
		assertTrue(Memory.INSTANCE.hadAQuery()); // the delete query
		assertFalse(store.exists(null, table, rowId));
	}
	
	@Test
	public void changeAndDelete() throws InterruptedException {
		WriteRetentionStore sut = sut50;
		Memory.INSTANCE.storeChanges(table, rowId, aChange, null, null);
		Memory.INSTANCE.resetQueries();

		sut.storeChanges(null, table, rowId, aChange, null, null);
		sut.delete(null, table, rowId);
		assertTrue(store.exists(null, table, rowId));
		Thread.sleep(10);
		assertTrue(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no delete
		Thread.sleep(90);
		assertTrue(Memory.INSTANCE.hadAQuery()); // the delete query
		assertFalse(store.exists(null, table, rowId));
	}
	
	@Test
	public void changeAndDeleteAndChange() throws InterruptedException {
		WriteRetentionStore sut = sut50;

		sut.storeChanges(null, table, rowId, aChange, null, null);
		sut.delete(null, table, rowId);
		sut.storeChanges(null, table, rowId, anotherChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(10);
		assertFalse(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no delete
		Thread.sleep(90);
		assertTrue(Memory.INSTANCE.hadAQuery()); // the store query
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test
	public void changeAndDeleteCell() throws InterruptedException {
		WriteRetentionStore sut = sut50;

		sut.storeChanges(null, table, rowId, aChange, null, null);
		sut.storeChanges(null, table, rowId, null, aDelete, null);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(10);
		assertFalse(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no delete
		Thread.sleep(90);
		assertTrue(Memory.INSTANCE.hadAQuery()); // the store query
		assertTrue(store.exists(null, table, rowId));
		assertFalse(store.exists(null, table, rowId, changedCf));
	}
	
	@Test
	public void changeAndDeleteAndChangeCell() throws InterruptedException {
		WriteRetentionStore sut = sut50;

		sut.storeChanges(null, table, rowId, aChange, null, null);
		sut.storeChanges(null, table, rowId, null, aDelete, null);
		sut.storeChanges(null, table, rowId, anotherChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(10);
		assertFalse(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no delete
		Thread.sleep(90);
		assertTrue(Memory.INSTANCE.hadAQuery()); // the store query
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test
	public void continuousWrite() {
		WriteRetentionStore sut = sut50;
		
		long start = System.currentTimeMillis();
		
		do {
			sut.storeChanges(null, table, rowId, aChange, null, null);
		} while(System.currentTimeMillis()-start < 80);
		assertEquals(1, Memory.INSTANCE.getQueriesAndReset()); // the only store query
		assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
		do {
			sut.storeChanges(null, table, rowId, anotherChange, null, null);
		} while(System.currentTimeMillis()-start < 140);
		this.waitForPendingRequests();
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test(timeout=10000)
	public void continuousWriteMultiThreaded() throws InterruptedException, ExecutionException {
		final WriteRetentionStore sut = sut50Mock;
		int parallelWrites = 200;

		final Object wait = new Object();
		final CountDownLatch waitForAllTasksToBeStarted = new CountDownLatch(parallelWrites);
		final CountDownLatch changing = new CountDownLatch(parallelWrites);
		final AtomicLong start = new AtomicLong();
		
		Runnable wr = new Runnable() {
			
			@Override
			public void run() {
				waitForAllTasksToBeStarted.countDown();
				synchronized(wait) {
					try {
						wait.wait();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				
				//Writing during at least 50ms so that a request is out
				do {
					sut.storeChanges(null, table, rowId, aChange, null, null);
				} while (System.currentTimeMillis()-start.get() < 50);
				
				changing.countDown();
				try {
					changing.await(500, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				sut.storeChanges(null, table, rowId, null, aDelete, null);
				sut.storeChanges(null, table, rowId, null, aDelete, null);
				sut.storeChanges(null, table, rowId, null, aDelete, null);
			}
		};

		Map<String, Set<String>> emptyRem = new TreeMap<String, Set<String>>();
		Map<String, Map<String, Number>> emptyInc = new TreeMap<String, Map<String,Number>>();
		InOrder inOrder = Mockito.inOrder(mockStore);

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int i = 0; i < parallelWrites; i++) {
			results.add(es.submit(wr));
		}
		es.shutdown();
		waitForAllTasksToBeStarted.await();
		
		synchronized(wait) {
			wait.notifyAll();
			start.set(System.currentTimeMillis());
		}
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}
		
		this.waitForPendingRequests();
		
		//Checking sent requests
		inOrder.verify(mockStore, Mockito.atLeastOnce()).storeChanges(null, table, rowId, aChange, emptyRem , emptyInc );
		inOrder.verify(mockStore, Mockito.atLeastOnce()).storeChanges(null, table, rowId, new DefaultColumnFamilyData(), aDelete, emptyInc);
		inOrder.verifyNoMoreInteractions();
	}
	
	@Test
	public void parrallelIncrements() throws Exception {
		final WriteRetentionStore sut = sut50;
		int parallelWrites = 200;
		
		final Object wait = new Object();
		final CountDownLatch waitForAllTasksToBeStarted = new CountDownLatch(parallelWrites);
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				waitForAllTasksToBeStarted.countDown();
				synchronized(wait) {
					try {
						wait.wait();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}

					sut.storeChanges(null, table, rowId, null, null, anIncrement);
				}
			}
		};

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int i = 0; i < parallelWrites; i++) {
			results.add(es.submit(r));
		}
		es.shutdown();
		waitForAllTasksToBeStarted.await();
		
		long start;
		synchronized(wait) {
			wait.notifyAll();
			start = System.currentTimeMillis();
		}
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}
		
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start;
		
		int q = Memory.INSTANCE.getQueriesAndReset();
		assertTrue(q > 0);
		assertEquals(duration/50, q, 2);
		assertEquals(Long.valueOf(parallelWrites), ConversionTools.convert(Long.class, Memory.INSTANCE.get(table, rowId, incrementedCf, incrementedKey)));
	}
}
