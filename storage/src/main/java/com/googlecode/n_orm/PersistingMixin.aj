package com.googlecode.n_orm;

import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;

public privileged aspect PersistingMixin {
	private static PersistingMixin INSTANCE;
	
	public static PersistingMixin getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}
	
	declare parents: (@Persisting *) implements PersistingElement;

	////If set within the constructor, we can't know whether the correct value is within the database, or that one that was set within the constructor.
	//declare error: set(!@Transient !@Key !transient !static (!Collection+) (PersistingElement+).*) && withincode((@Persisting *).new(..)) : "Only key attribute should be set within a persisting constructor.";

	declare error : set(!@Transient static !transient !final * PersistingElement+.*) : "Static fields can only be transient in persisting classes";
	declare error: set(!@Transient final !transient !static * PersistingElement+.*) : "Final fields must be transient";
	
	pointcut creation(PersistingElement self): execution(PersistingElement+.new(..)) && this(self) && if(thisJoinPointStaticPart.getSignature().getDeclaringType().equals(self.getClass()));
	
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
		
		return this.getIdentifier().equals(((PersistingElement)rhs).getIdentifier());
	}
	
	public int PersistingElement.hashCode() {
		return this.getFullIdentifier().hashCode();
	}
	
	public int PersistingElement.compareTo(PersistingElement rhs) {
		if (rhs == null)
			return 1;
		int clsCmp = this.getClass().getName().compareTo(rhs.getClass().getName());
		if (clsCmp != 0)
			return clsCmp;
		
		return this.getIdentifier().compareTo(((PersistingElement)rhs).getIdentifier());
	}
	
	public String PersistingElement.toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.getClass().getName());
		String ident = this.getIdentifier();
		if (ident == null) {
			sb.append(" with no key yet (missing some key values)");
		} else {
			ident = ident.replace(KeyManagement.KEY_SEPARATOR, ":");
			ident = ident.replace(KeyManagement.KEY_END_SEPARATOR, "}");
			ident = ident.replace(KeyManagement.ARRAY_SEPARATOR, ";");
			sb.append(" with key " + ident);
		}
		return sb.toString();
	}
	
}
