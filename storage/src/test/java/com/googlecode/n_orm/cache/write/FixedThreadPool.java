package com.googlecode.n_orm.cache.write;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FixedThreadPool implements ExecutorService {
	private final int threadNumber;
	private final AtomicInteger remainingSubmittableThreads;
	private final ConcurrentHashMap<Thread, Future<?>> threads = new ConcurrentHashMap<Thread, Future<?>>();
	private volatile boolean done = false;
	private final CountDownLatch starterLatch;
	
	public FixedThreadPool(int threadNumber) {
		super();
		this.threadNumber = threadNumber;
		this.remainingSubmittableThreads = new AtomicInteger(threadNumber);
		starterLatch = new CountDownLatch(threadNumber);
	}

	@Override
	public void execute(Runnable command) {
		this.submit(command);
	}

	@Override
	public void shutdown() {
		this.done = true;
	}

	@Override
	public List<Runnable> shutdownNow() {
		this.done = true;
		List<Runnable> ret = new LinkedList<Runnable>();
		for (Thread t : this.threads.keySet()) {
			if (t.isAlive() && !t.isInterrupted())
				t.interrupt();
			ret.add(t);
		}
		return ret;
	}

	@Override
	public boolean isShutdown() {
		return ! done;
	}

	@Override
	public boolean isTerminated() {
		return this.threads.isEmpty();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return false;
	}

	@Override
	public <T> Future<T> submit(final Callable<T> task) {
		if (this.done)
			throw new RuntimeException("Cannot submit another task");
		final int submissionNumber = this.remainingSubmittableThreads.decrementAndGet();
		if (submissionNumber < 0)
			throw new RuntimeException("Cannot submit another task");
		if (this.done)
			throw new RuntimeException("Cannot submit another task");
		final AtomicReference<T> res = new AtomicReference<T>();
		final AtomicReference<Exception> ex = new AtomicReference<Exception>();
		final AtomicBoolean done = new AtomicBoolean(false);
		final AtomicBoolean canceled = new AtomicBoolean(false);
		final Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					if (canceled.get())
						return;
					starterLatch.countDown();
					if (canceled.get())
						return;
					starterLatch.await();
					if (canceled.get())
						return;
					res.compareAndSet(null, task.call());
				} catch (Exception e) {
					ex.compareAndSet(null, e);
				} finally {
					synchronized(done) {
						done.compareAndSet(false, true);
						done.notifyAll();
					}
					threads.remove(Thread.currentThread());
				}
			}
		});
		Future<T> ret = new Future<T>() {

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				if (!canceled.compareAndSet(false, true))
					return false;
				
				if (mayInterruptIfRunning && t.isAlive())
					t.interrupt();
				
				return true;
			}

			@Override
			public boolean isCancelled() {
				return canceled.get();
			}

			@Override
			public boolean isDone() {
				return done.get();// || writeRetentionTest.done;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				if (done.get() == false) {
					synchronized(done) {
						if (done.get() == false) {
							done.wait();
						}
					}
				}
				if (ex.get() != null)
					throw new ExecutionException(ex.get());
				
				return res.get();
			}

			@Override
			public T get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				if (done.get() == false) {
					synchronized(done) {
						if (done.get() == false) {
							done.wait(TimeUnit.MILLISECONDS.convert(timeout, unit));
						}
					}
				}
				if (ex.get() != null)
					throw new ExecutionException(ex.get());
				
				return res.get();
			}
		};
		t.start();
		this.threads.putIfAbsent(t, ret);
		return ret;
	}

	@Override
	public <T> Future<T> submit(final Runnable task, final T result) {
		return this.submit(new Callable<T>() {

			@Override
			public T call() throws Exception {
				task.run();
				return result;
			}
		});
	}

	@Override
	public Future<?> submit(Runnable task) {
		return this.submit(task, null);
	}

	@Override
	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout,
			TimeUnit unit) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		// TODO Auto-generated method stub
		return null;
	}
	
}