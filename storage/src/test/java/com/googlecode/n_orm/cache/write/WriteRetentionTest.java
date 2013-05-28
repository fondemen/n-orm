package com.googlecode.n_orm.cache.write;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.Transient;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.memory.Memory;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;
import com.googlecode.n_orm.storeapi.Store;

public class WriteRetentionTest {
	
	private static class SlowWriteDelegatingStore extends DelegatingStore {
		/** The set of requests being sending */
		@Transient private Set<String> sending = Collections.synchronizedSet(new HashSet<String>());

		private SlowWriteDelegatingStore(Store actualStore) {
			super(actualStore);
		}

		@Override
		public void delete(MetaInformation meta, String table, String id)
				throws DatabaseNotReachedException {
			assert sending.add(table+'/'+id) : "request on table " + table + " with id " + id + " running twice";
			try {
				Thread.sleep(500);
			} catch (InterruptedException x) {
				assert false;
			}
			super.delete(meta, table, id);
			assert sending.remove(table+'/'+id);
		}

		@Override
		public void storeChanges(MetaInformation meta, String table,
				String id, ColumnFamilyData changed,
				Map<String, Set<String>> removed,
				Map<String, Map<String, Number>> increments)
				throws DatabaseNotReachedException {
			assert sending.add(table+'/'+id) : "request on table " + table + " with id " + id + " running twice";
			try {
				Thread.sleep(500);
			} catch (InterruptedException x) {
				assert false;
			}
			super.storeChanges(meta, table, id, changed, removed, increments);
			assert sending.remove(table+'/'+id);
		}
		
	}
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
	private static WriteRetentionStore sutSlowDS;
	private static WriteRetentionStore sutSlowMock;
	private static Store store = SimpleStoreWrapper.getWrapper(Memory.INSTANCE);
	private static Store mockStore = Mockito.mock(Store.class);
	
	@BeforeClass
	public static void setupPossibleSuts() {
		WriteRetentionStore.setCapureHitRatio(true);
		
		sut50Mock = WriteRetentionStore.getWriteRetentionStore(1, mockStore);
		sut50Mock.start();
		sut50 = WriteRetentionStore.getWriteRetentionStore(50, store);
		sut50.start();
		sut200 = WriteRetentionStore.getWriteRetentionStore(200, store);
		sut200.start();

		sutSlowDS = WriteRetentionStore.getWriteRetentionStore(50, new SlowWriteDelegatingStore(store));
		sutSlowDS.start();
		
		sutSlowMock = WriteRetentionStore.getWriteRetentionStore(50, new SlowWriteDelegatingStore(mockStore));
		sutSlowMock.start();
		
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
	
	@AfterClass
	public static void printStats() {
		System.out.println("Average latency " + WriteRetentionStore.getAverageLatencyMs());
		System.out.println("Cumulative latency " + WriteRetentionStore.getCumulativeLatencyMs());
		System.out.println("Counted latencies " + WriteRetentionStore.getLatencySamples());
		System.out.println("Requests in " + WriteRetentionStore.getRequestsIn());
		System.out.println("Requests out " + WriteRetentionStore.getRequestsOut());
		System.out.println("hit ratio " + (100*WriteRetentionStore.getHitRatio()) + '%');
		System.out.println("Max threads " + ManagementFactory.getThreadMXBean().getPeakThreadCount());
	}
	
	@Before
	public void cleanupStore() {
		Memory.INSTANCE.reset();
	}
	
	@Before
	public void resetMock() {
		Mockito.reset(mockStore);
	}
	
	@Before
	public void checkEnabled() {
		if (!sut50.isEnabledByDefault()) sut50.setEnabledByDefault(true);
		if (!sut50Mock.isEnabledByDefault()) sut50Mock.setEnabledByDefault(true);
		if (!sut200.isEnabledByDefault()) sut200.setEnabledByDefault(true);
		if (!WriteRetentionStore.isEnabledForCurrentThread())
			WriteRetentionStore.setEnabledForCurrentThread(true);
	}

	@After
	public void waitForPendingRequests() {
		try {
			int maxTurns = 3000;
			while(WriteRetentionStore.getPendingRequests() != 0) {
				if (maxTurns % 10 == 0)
					assertTrue(WriteRetentionStore.getActiveSenderThreads() <= WriteRetentionStore.getMaxSenderThreads());
				Thread.sleep(10);
				maxTurns--;
				if (maxTurns <= 0)
					throw new IllegalStateException();
			}
			Thread.sleep(10);
			while(WriteRetentionStore.getPendingRequests() != 0) {
				if (maxTurns % 10 == 0)
					assertTrue(WriteRetentionStore.getActiveSenderThreads() <= WriteRetentionStore.getMaxSenderThreads());
				Thread.sleep(10);
				maxTurns--;
				if (maxTurns <= 0)
					throw new IllegalStateException();
			}
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
	
	@Test(timeout=10000)
	public void getAlwaysSameRetensionStore() throws InterruptedException, ExecutionException {
		final int parallelGets = 100;
		final Store s = Mockito.mock(Store.class);
		Callable<WriteRetentionStore> getWRS = new Callable<WriteRetentionStore>() {

			@Override
			public WriteRetentionStore call() throws Exception {
				return WriteRetentionStore.getWriteRetentionStore(1234, s);
			}
		};
		ExecutorService es = new FixedThreadPool(parallelGets);
		Collection<Future<WriteRetentionStore>> results = new LinkedList<Future<WriteRetentionStore>>();
		for (int i = 0; i < parallelGets; i++) {
			results.add(es.submit(getWRS));
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
	
	@Test(timeout=10000)
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
	
	@Test(timeout=10000)
	public void disableForThread() throws InterruptedException {
		WriteRetentionStore sut = sut200;
		sut.setEnabledByDefault(false);
		
		WriteRetentionStore.setEnabledForCurrentThread(false);
		
		sut.storeChanges(null, table, rowId, aChange, null, null);
		assertTrue(store.exists(null, table, rowId));
		sut.delete(null, table, rowId);
		assertFalse(store.exists(null, table, rowId));
		
		WriteRetentionStore.setEnabledForCurrentThread(true);
		
		sut.storeChanges(null, table, rowId, aChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(100);
		assertFalse(store.exists(null, table, rowId));
		Thread.sleep(150);
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test(timeout=10000)
	public void disableByDefaultForThread() throws Throwable {
		final WriteRetentionStore sut = sut200;
		
		sut.setEnabledByDefault(false);
		
		Thread t = new Thread() {

			@Override
			public void run() {
				try {
					sut.storeChanges(null, table, rowId, aChange, null, null);
					assertTrue(store.exists(null, table, rowId));
					sut.delete(null, table, rowId);
					assertFalse(store.exists(null, table, rowId));
					
					WriteRetentionStore.setEnabledForCurrentThread(true);
					
					sut.storeChanges(null, table, rowId, aChange, null, null);
					assertFalse(store.exists(null, table, rowId));
					Thread.sleep(100);
					assertFalse(store.exists(null, table, rowId));
					Thread.sleep(150);
					assertTrue(store.exists(null, table, rowId));
					assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			
		};
		final Throwable[] x = new Throwable[1];
		t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				x[0] = e;
			}
		});
		t.start();
		t.join();
		if (x[0] != null)
			throw x[0];
	}
	
	@Test(timeout=10000)
	public void flush() throws InterruptedException {
		WriteRetentionStore sut = sut200;
		
		sut.storeChanges(null, table, rowId, aChange, null, null);
		assertFalse(store.exists(null, table, rowId));
		sut.flush(table, rowId);
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Persisting(writeRetentionMs=100)
	public static class Element {
		private static final long serialVersionUID = 6304132311755611785L;
		@Key public String key;
		public String value;
	}
	
	@Test(timeout=10000)
	public void storeFromAPI() throws InterruptedException {
		Element e = new Element(); e.key = rowId;
		assertEquals(WriteRetentionStore.class, e.getStore().getClass());
		
		e.store();
		assertFalse(e.existsInStore());
		
		this.waitForPendingRequests();
		assertTrue(e.existsInStore());
	}
	
	@Test(timeout=10000)
	public void flushFromAPI() throws InterruptedException {
		Element e = new Element(); e.key = rowId;
		assertEquals(WriteRetentionStore.class, e.getStore().getClass());
		
		e.storeNoCache();
		assertTrue(e.existsInStore());
	}
	
	@Test(timeout=10000)
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
	
	@Test(timeout=10000)
	public void delete() throws InterruptedException {
		WriteRetentionStore sut = sut50;
		Memory.INSTANCE.storeChanges(table, rowId, aChange, null, null);
		Memory.INSTANCE.resetQueries();
		
		sut.delete(null, table, rowId);
		assertTrue(store.exists(null, table, rowId));
		Thread.sleep(10);
		assertTrue(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // 2 exists ; no delete
		this.waitForPendingRequests();
		assertFalse(store.exists(null, table, rowId));
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // the delete and the exists queries
	}
	
	@Test(timeout=10000)
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
	
	@Test(timeout=10000)
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
		assertEquals(2, Memory.INSTANCE.getQueriesAndReset()); // the delete followed by the store query
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test(timeout=10000)
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
	
	@Test(timeout=10000)
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
	
	@Test(timeout=10000)
	public void deleteAndChangeExisting() throws InterruptedException {
		WriteRetentionStore sut = sut50;

		Memory.INSTANCE.storeChanges(table, rowId, aChange, null, null);
		Memory.INSTANCE.resetQueries();
		sut.delete(null, table, rowId);
		sut.storeChanges(null, table, rowId, null, null, anIncrement);
		Thread.sleep(90);
		//assertTrue(Memory.INSTANCE.hadAQuery()); // the store query
		assertTrue(store.exists(null, table, rowId));
		assertArrayEquals(ConversionTools.convert(1L), store.get(null, table, rowId, incrementedCf, incrementedKey));
		assertNull(store.get(null, table, rowId, changedCf, changedKey));
	}
	
	@Test(timeout=5000)
	public void continuousWrite() {
		WriteRetentionStore sut = sut50;
		
		long start = System.currentTimeMillis();
		long askedQueries = 0;
		long sentQueries = 0;
		
		do {
			sut.storeChanges(null, table, rowId, aChange, null, null);
			askedQueries++;
		} while(System.currentTimeMillis()-start < 80);
		int memQueries = 0;
		//Waiting for a query to be sent
		while((memQueries = Memory.INSTANCE.getQueriesAndReset()) == 0) {
			sut.storeChanges(null, table, rowId, aChange, null, null);
			askedQueries++;
			Thread.yield();
		}
		sentQueries += memQueries;
		// Checking correct values are stored
		assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
		do {
			sut.storeChanges(null, table, rowId, anotherChange, null, null);
			askedQueries++;
		} while(System.currentTimeMillis()-start < 140);
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start;
		sentQueries += Memory.INSTANCE.getQueriesAndReset();
		
		assertTrue(sentQueries < askedQueries);
		assertTrue(sentQueries <= (1+(duration/50)));
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
		//System.out.println("sent " + sentQueries + "(expected " + (1+(duration/50)) + ") ; asked " + askedQueries);
	}
	
	@Test(timeout=5000)
	public void continuousWriteOnSlowDS() {
		WriteRetentionStore sut = sutSlowDS;
		
		long start = System.currentTimeMillis();
		long askedQueries = 0;
		long sentQueries = 0;
		
		do {
			sut.storeChanges(null, table, rowId, aChange, null, null);
			askedQueries++;
		} while(System.currentTimeMillis()-start < 80);
		int memQueries = 0;
		//Waiting for a query to be sent
		while((memQueries = Memory.INSTANCE.getQueriesAndReset()) == 0) {
			sut.storeChanges(null, table, rowId, aChange, null, null);
			askedQueries++;
			Thread.yield();
		}
		sentQueries += memQueries;
		// Checking correct values are stored
		while (store.get(null, table, rowId, changedCf, changedKey)==null) Thread.yield();
		assertArrayEquals(changedValue1, store.get(null, table, rowId, changedCf, changedKey));
		do {
			sut.storeChanges(null, table, rowId, anotherChange, null, null);
			askedQueries++;
		} while(System.currentTimeMillis()-start < 140);
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start;
		sentQueries += Memory.INSTANCE.getQueriesAndReset();
		
		assertTrue(sentQueries < askedQueries);
		assertTrue(sentQueries <= (1+(duration/50)));
		assertArrayEquals(changedValue2, store.get(null, table, rowId, changedCf, changedKey));
		//System.out.println("sent " + sentQueries + "(expected " + (1+(duration/50)) + ") ; asked " + askedQueries);
	}
	
	@Test(timeout=10000)
	public void continuousWriteMultiThreaded() throws InterruptedException, ExecutionException {
		final WriteRetentionStore sut = sut50Mock;
		int parallelWrites = 200;

		final CountDownLatch changing = new CountDownLatch(parallelWrites);
		final AtomicLong start = new AtomicLong();
		
		Runnable wr = new Runnable() {
			
			@Override
			public void run() {
				
				start.compareAndSet(0, System.currentTimeMillis());
				
				//Writing during at least 50ms so that a request is out
				while (System.currentTimeMillis()-start.get() < 60) {
					sut.storeChanges(null, table, rowId, aChange, null, null);
				}
				
				changing.countDown();
				try {
					changing.await();
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
		ExecutorService es = new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			results.add(es.submit(wr));
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}

		this.waitForPendingRequests();
		Thread.yield();
		this.waitForPendingRequests();
		
		//Checking sent requests
		inOrder.verify(mockStore, Mockito.atLeastOnce()).storeChanges(null, table, rowId, aChange, emptyRem , emptyInc );
		inOrder.verify(mockStore, Mockito.atLeastOnce()).storeChanges(null, table, rowId, new DefaultColumnFamilyData(), aDelete, emptyInc);
		inOrder.verifyNoMoreInteractions();
	}
	
	@Test(timeout=10000)
	public void continuousWriteMultiThreadedOnSLowDS() throws InterruptedException, ExecutionException {
		final WriteRetentionStore sut = sutSlowMock;
		int parallelWrites = 200;

		final CountDownLatch changing = new CountDownLatch(parallelWrites);
		final AtomicLong start = new AtomicLong();
		
		Runnable wr = new Runnable() {
			
			@Override
			public void run() {
				
				start.compareAndSet(0, System.currentTimeMillis());
				
				//Writing during at least 50ms so that a request is out
				while (System.currentTimeMillis()-start.get() < 60) {
					sut.storeChanges(null, table, rowId, aChange, null, null);
				}
				
				changing.countDown();
				try {
					changing.await();
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
		ExecutorService es = new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			results.add(es.submit(wr));
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}

		this.waitForPendingRequests();
		Thread.yield();
		this.waitForPendingRequests();
		
		//Checking sent requests
		inOrder.verify(mockStore, Mockito.atLeastOnce()).storeChanges(null, table, rowId, aChange, emptyRem , emptyInc );
		inOrder.verify(mockStore, Mockito.atLeastOnce()).storeChanges(null, table, rowId, new DefaultColumnFamilyData(), aDelete, emptyInc);
		inOrder.verifyNoMoreInteractions();
	}
	
	@Test(timeout=10000)
	public void parrallelIncrements() throws Exception {
		WriteRetentionStore.setCapureHitRatio(true);
		
		final WriteRetentionStore sut = sut50;
		int parallelWrites = 200;
		final AtomicLong start = new AtomicLong();
		
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				start.compareAndSet(0, System.currentTimeMillis());
				sut.storeChanges(null, table, rowId, null, null, anIncrement);
			}
		};

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es =  new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			results.add(es.submit(r));
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}
		
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start.get();
		
		int q = Memory.INSTANCE.getQueriesAndReset();
		assertEquals(Long.valueOf(parallelWrites), ConversionTools.convert(Long.class, Memory.INSTANCE.get(table, rowId, incrementedCf, incrementedKey)));
		assertTrue(q > 0);
		//assertTrue(1+2*(duration/50) > q);
		System.out.println("sent: " + q + " ; asked " + parallelWrites + " ; duration " + duration);
	}
	
	@Test(timeout=10000)
	public void parrallelIncrementsOnSlowDataStore() throws Exception {
		WriteRetentionStore.setCapureHitRatio(true);
		
		final WriteRetentionStore sut = sutSlowDS;
		int parallelWrites = 200;
		final AtomicLong start = new AtomicLong();
		
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				start.compareAndSet(0, System.currentTimeMillis());
				sut.storeChanges(null, table, rowId, null, null, anIncrement);
			}
		};

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es =  new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			results.add(es.submit(r));
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}
		
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start.get();
		
		int q = Memory.INSTANCE.getQueriesAndReset();
		assertEquals(Long.valueOf(parallelWrites), ConversionTools.convert(Long.class, Memory.INSTANCE.get(table, rowId, incrementedCf, incrementedKey)));
		assertTrue(q > 0);
		//assertTrue(1+2*(duration/50) > q);
		System.out.println("sent: " + q + " ; asked " + parallelWrites + " ; duration " + duration);
	}
	
	@Test(timeout=10000)
	public void parrallelIncrementsWithSomeFlushes() throws Exception {
		final WriteRetentionStore sut = sut50;
		int parallelWrites = 200;
		final AtomicLong start = new AtomicLong();
		
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				start.compareAndSet(0, System.currentTimeMillis());
				sut.storeChanges(null, table, rowId, null, null, anIncrement);
			}
		};
		
		Runnable rf = new Runnable() {
			
			@Override
			public void run() {
				start.compareAndSet(0, System.currentTimeMillis());
				sut.storeChanges(null, table, rowId, null, null, anIncrement);
				sut.flush(table, rowId);
			}
		};

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es =  new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			if (i%10 != 0) {
				results.add(es.submit(r));
			} else {
				results.add(es.submit(rf));
			}
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}
		
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start.get();
		
		int q = Memory.INSTANCE.getQueriesAndReset();
		assertTrue(q > 0);
		assertEquals(Long.valueOf(parallelWrites), ConversionTools.convert(Long.class, Memory.INSTANCE.get(table, rowId, incrementedCf, incrementedKey)));
		assertTrue(1+2*(duration/50)+parallelWrites/10 > q);
		//System.out.println("sent: " + q +" (expected " + (parallelWrites/10 + duration/50) + ") ; asked " + parallelWrites);
	}
	
	@Test//(timeout=10000)
	public void parrallelIncrementsWithSomeFlushesOnSlowDS() throws Exception {
		final WriteRetentionStore sut = sutSlowDS;
		int parallelWrites = 200;
		final AtomicLong start = new AtomicLong();
		
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				start.compareAndSet(0, System.currentTimeMillis());
				sut.storeChanges(null, table, rowId, null, null, anIncrement);
			}
		};
		
		Runnable rf = new Runnable() {
			
			@Override
			public void run() {
				start.compareAndSet(0, System.currentTimeMillis());
				sut.storeChanges(null, table, rowId, null, null, anIncrement);
				sut.flush(table, rowId);
			}
		};

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es =  new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			if (i%10 != 0) {
				results.add(es.submit(r));
			} else {
				results.add(es.submit(rf));
			}
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}
		
		this.waitForPendingRequests();
		long duration = System.currentTimeMillis()-start.get();
		
		int q = Memory.INSTANCE.getQueriesAndReset();
		assertTrue(q > 0);
		assertEquals(Long.valueOf(parallelWrites), ConversionTools.convert(Long.class, Memory.INSTANCE.get(table, rowId, incrementedCf, incrementedKey)));
		assertTrue(1+2*(duration/50)+parallelWrites/10 > q);
		//System.out.println("sent: " + q +" (expected " + (parallelWrites/10 + duration/50) + ") ; asked " + parallelWrites);
	}
	
	@Test(timeout=10000)
	public void continuousWriteMultiThreadedOnDifferentKeys() throws InterruptedException, ExecutionException {
		final WriteRetentionStore sut = sut50;
		int parallelWrites = 200;

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es = new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			final int nr = i;
			results.add(es.submit(new Runnable() {
				
				@Override
				public void run() {
					String id = rowId + nr;
					
					sut.storeChanges(null, table, id, aChange, null, null);
				}
			}));
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}

		this.waitForPendingRequests();
		Thread.yield();
		this.waitForPendingRequests();
		
		//Checking all ids are here
		for(int i = 0; i < parallelWrites; ++i) {
			String id = rowId+i;
			boolean exists = Memory.INSTANCE.exists(table, id);
			if (! exists)
				this.waitForPendingRequests();
				
			assertTrue("Can't find element " + id, Memory.INSTANCE.exists(table, id));
		}
		assertFalse(Memory.INSTANCE.exists(table, rowId+parallelWrites));
		assertFalse(Memory.INSTANCE.exists(table, rowId+(parallelWrites+1)));
	}
	
	@Test(timeout=10000)
	public void continuousWriteMultiThreadedOnDifferentKeysOnSlowDataStore() throws InterruptedException, ExecutionException {
		this.continuousWriteMultiThreadedOnDifferentKeysOnSlowDataStore(200);
	}

	public void continuousWriteMultiThreadedOnDifferentKeysOnSlowDataStore(int parallelWrites) throws InterruptedException, ExecutionException {
		final WriteRetentionStore sut = sutSlowDS;

		Collection<Future<?>> results = new LinkedList<Future<?>>();
		ExecutorService es = new FixedThreadPool(parallelWrites);
		for (int i = 0; i < parallelWrites; i++) {
			final int nr = i;
			results.add(es.submit(new Runnable() {
				
				@Override
				public void run() {
					String id = rowId + nr;
					
					sut.storeChanges(null, table, id, aChange, null, null);
				}
			}));
		}
		es.shutdown();
		
		//Waiting for results and checking exceptions
		for (Future<?> f : results) {
			f.get();
		}

		this.waitForPendingRequests();
		Thread.yield();
		this.waitForPendingRequests();
		
		//Checking all ids are here
		for(int i = 0; i < parallelWrites; ++i) {
			String id = rowId+i;
			boolean exists = Memory.INSTANCE.exists(table, id);
			if (! exists)
				this.waitForPendingRequests();
				
			assertTrue("Can't find element " + id, Memory.INSTANCE.exists(table, id));
		}
		assertFalse(Memory.INSTANCE.exists(table, rowId+parallelWrites));
		assertFalse(Memory.INSTANCE.exists(table, rowId+(parallelWrites+1)));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void zeroMaxThreadsSetAttempt() {
		WriteRetentionStore.setMaxSenderThreads(0);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void negativeMaxThreadsSetAttempt() {
		WriteRetentionStore.setMaxSenderThreads(-1);
	}
	
	@Test(timeout=20000)
	public void continuousWriteMultiThreadedOnDifferentKeysOnSlowDataStoreAndLowParrallelSenderThreads() throws InterruptedException, ExecutionException {
		int originalMaxThreads = WriteRetentionStore.getMaxSenderThreads();
		
		WriteRetentionStore.setMaxSenderThreads(5);
		
		try {
			this.continuousWriteMultiThreadedOnDifferentKeysOnSlowDataStore(50);
		} finally {
			WriteRetentionStore.setMaxSenderThreads(originalMaxThreads);
		}
	}
}
