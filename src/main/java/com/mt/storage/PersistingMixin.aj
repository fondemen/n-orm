package com.mt.storage;

import java.lang.reflect.Field;



public privileged aspect PersistingMixin {
	private static PersistingMixin INSTANCE;
	
	public static PersistingMixin getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}
	
	declare parents: (@Persisting *) implements PersistingElement;

	////If set within the constructor, we can't know whether the correct value is within the database, or that one that was set within the constructor.
	//declare error: set(!@Key !transient !static (!Collection+) (PersistingElement+).*) && withincode((@Persisting *).new(..)) : "Only key attribute should be set within a persisting constructor.";

	declare error : set(static !transient !final * PersistingElement+.*) : "Static fields can only be transient in persisting classes";
	
	pointcut creation(PersistingElement self): execution(com.mt.storage.PersistingElement+.new(..)) && this(self) && if(thisJoinPointStaticPart.getSignature().getDeclaringType().equals(self.getClass()));
	
	before(PersistingElement self): PersistingMixin.creation(self) {
		if (! self.getClass().isAnnotationPresent(Persisting.class))
			throw new IllegalStateException("Class " + self.getClass() + " implements " + PersistingElement.class +" instead of declaring annotation " + Persisting.class);
		
		KeyManagement.getInstance().detectKeys(self.getClass());
	}
	
	public String getTable(Class<? extends PersistingElement> clazz) {
		Persisting pa = clazz.getAnnotation(Persisting.class);
		if (pa == null)
			throw new IllegalStateException("Persisting " + clazz + " must be annotated with " + Persisting.class);
		String ret = pa.table();
		return ret.length() == 0 ? clazz.getName() : ret;
	}
	
	public String PersistingElement.getTable() {
		return PersistingMixin.getInstance().getTable(this.getClass());
	}
	
	public boolean PersistingElement.equals(Object rhs) {
		if (rhs == null)
			return false;
		if (!this.getClass().equals(rhs.getClass()))
			return false;
		
		PropertyManagement pm = PropertyManagement.getInstance();
		for (Field key : KeyManagement.getInstance().detectKeys(this.getClass())) {
			if (! pm.candideReadValue(this, key).equals(pm.candideReadValue(rhs, key)))
				return false;
		}
		
		return true;
	}
	
}
