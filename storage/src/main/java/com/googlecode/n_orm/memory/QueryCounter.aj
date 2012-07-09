package com.googlecode.n_orm.memory;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import org.aspectj.lang.reflect.MethodSignature;

public aspect QueryCounter {
	private AtomicInteger Memory.queries = new AtomicInteger();
	
	public void Memory.resetQueries() {
		this.queries.set(0);
	}
	
	public int Memory.getQueriesAndReset() {
		return this.queries.getAndSet(0);
	}
	
	public boolean Memory.hadAQuery() {
		return this.getQueriesAndReset() == 1;
	}
	
	public boolean Memory.hadNoQuery() {
		return this.getQueriesAndReset() == 0;
	}
	
	private transient volatile Method Memory.running = null;
	
	protected pointcut runningQuery(Memory self) : execution(* com.googlecode.n_orm.storeapi.SimpleStore.*(..)) && !execution(void com.googlecode.n_orm.storeapi.SimpleStore.start()) && target(self);
	
	before(Memory self): runningQuery(self) && if(self.running == null) {
		self.running = ((MethodSignature)thisJoinPointStaticPart.getSignature()).getMethod();
		self.queries.incrementAndGet();
	}
	
	after(Memory self): runningQuery(self) {
		Method m = ((MethodSignature)thisJoinPointStaticPart.getSignature()).getMethod();
		if (m.equals(self.running))
			self.running = null;
	}
	
	after(Memory self) returning: execution(public void reset()) && this(self) {
		self.resetQueries();
	}
}
