package com.googlecode.n_orm.memory;

import java.lang.reflect.Method;

import org.aspectj.lang.reflect.MethodSignature;

public aspect QueryCounter {
	private int Memory.queries = 0;
	
	public void Memory.resetQueries() {
		this.queries = 0;
	}
	
	public boolean Memory.hadAQuery() {
		boolean ret = this.queries == 1;
		this.resetQueries();
		return ret;
	}
	
	public boolean Memory.hadNoQuery() {
		boolean ret = this.queries == 0;
		this.resetQueries();
		return ret;
	}
	
	private transient volatile Method Memory.running = null;
	
	protected pointcut runningQuery(Memory self) : execution(* com.googlecode.n_orm.storeapi.Store.*(..)) && !execution(void com.googlecode.n_orm.storeapi.Store.start()) && target(self);
	
	before(Memory self): runningQuery(self) && if(self.running == null) {
		self.running = ((MethodSignature)thisJoinPointStaticPart.getSignature()).getMethod();
		self.queries++;
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
