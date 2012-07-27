package com.googlecode.n_orm.memory;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections.map.AbstractTestSortedMap.TestTailMap;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.memory.EvictionTest.RowTs;
import com.googlecode.n_orm.storeapi.Row;

public class EvictionTest {
	static long Precision = 15;
	
	String table = "testTable", row = "testRow";
	
	Memory sut;
	EvictionListener l;
	BlockingQueue<RowTs> evictedRows;
	
	public class EL implements EvictionListener {

		@Override
		public void rowEvicted(Row row) {
			evictedRows.add(new RowTs(row, System.currentTimeMillis()));
		}
		
	}
	
	public class RowTs {
		public final Row row;
		public final long ts;
		private RowTs(Row row, long ts) {
			super();
			this.row = row;
			this.ts = ts;
		}
	}

	private long now() {
		return System.currentTimeMillis();
	}
	
	@Before
	public void setupMemory() throws InterruptedException {
		sut = new Memory();
		l = new EL();
		evictedRows = new LinkedBlockingQueue<RowTs>();
		sut.addEvictionListener(l);
		
		//Setting up the system
		sut.storeChanges(table, row, null, null, null, 10L);
		evictedRows.take();
		assertTrue(evictedRows.isEmpty());
	}

	@Test(timeout=1000)
	public void register50ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row, null, null, null, 50L);
		RowTs r = evictedRows.take();
		assertEquals(50, r.ts - now, 10);
	}

	@Test(timeout=1000)
	public void register1ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row, null, null, null, 1L);
		RowTs r = evictedRows.take();
		assertEquals(5, r.ts - now, 5);
	}

	@Test(timeout=1000, expected=IllegalArgumentException.class)
	public void register0ms() throws InterruptedException {
		sut.storeChanges(table, row, null, null, null, 0L);
	}

	@Test(timeout=1000, expected=IllegalArgumentException.class)
	public void registerMinusXms() throws InterruptedException {
		sut.storeChanges(table, row, null, null, null, -11L);
	}

	@Test(timeout=1000)
	public void register50n50ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row + 1, null, null, null, 50L);
		sut.storeChanges(table, row + 2, null, null, null, 50L);
		RowTs r = evictedRows.take();
		assertEquals(50, r.ts - now, 10);
		r = evictedRows.take();
		assertEquals(50, r.ts - now, 10);
	}

	@Test(timeout=1000)
	public void register100n50ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row + 1, null, null, null, 100L);
		sut.storeChanges(table, row + 2, null, null, null, 50L);
		RowTs r = evictedRows.take();
		assertEquals(50, r.ts - now, 10);
		assertEquals(row + 2, r.row.getKey());
		r = evictedRows.take();
		assertEquals(100, r.ts - now, 10);
		assertEquals(row + 1, r.row.getKey());
	}

	@Test(timeout=1000)
	public void register50n100ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row + 1, null, null, null, 50L);
		sut.storeChanges(table, row + 2, null, null, null, 100L);
		RowTs r = evictedRows.take();
		assertEquals(50, r.ts - now, 10);
		assertEquals(row + 1, r.row.getKey());
		r = evictedRows.take();
		assertEquals(100, r.ts - now, 10);
		assertEquals(row + 2, r.row.getKey());
	}

	@Test(timeout=1000)
	public void register50then100ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row + 1, null, null, null, 50L);
		Thread.sleep(25);
		sut.storeChanges(table, row + 2, null, null, null, 100L);
		RowTs r = evictedRows.take();
		assertEquals(50, r.ts - now, 10);
		assertEquals(row + 1, r.row.getKey());
		r = evictedRows.take();
		assertEquals(125, r.ts - now, 10);
		assertEquals(row + 2, r.row.getKey());
	}

	@Test(timeout=1000)
	public void register100then50ms() throws InterruptedException {
		long now = now();
		sut.storeChanges(table, row + 1, null, null, null, 100L);
		Thread.sleep(25);
		sut.storeChanges(table, row + 2, null, null, null, 50L);
		RowTs r = evictedRows.take();
		assertEquals(75, r.ts - now, 10);
		assertEquals(row + 2, r.row.getKey());
		r = evictedRows.take();
		assertEquals(100, r.ts - now, 10);
		assertEquals(row + 1, r.row.getKey());
	}
}
