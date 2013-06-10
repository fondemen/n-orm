package com.googlecode.n_orm;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.consoleannotations.Continuator;

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
	
	private transient Map<String /*class name*/, String /*table*/> tables = Collections.emptyMap();
	
	public String getTable(Class<? extends PersistingElement> clazz) {
		String ret = this.tables.get(clazz.getName());
		if (ret == null) {
			Persisting pa = clazz.getAnnotation(Persisting.class);
			if (pa == null)
				throw new IllegalStateException("Persisting " + clazz + " must be annotated with " + Persisting.class);
			ret = pa.table();
			ret = ret.length() == 0 ? clazz.getName() : ret;
			Map<String, String> newTablesCache = new TreeMap<String, String>(tables);
			newTablesCache.put(clazz.getName(), ret);
			this.tables = newTablesCache;
		}
		return ret;
	}
	
	private transient String PersistingElement.table;
	
	public String PersistingElement.getTable() {
		if (this.table == null) {
			this.table = PersistingMixin.getInstance().getTable(this.getClass());
		}
		return this.table;
	}
	
	public boolean PersistingElement.equals(Object rhs) {
		if (rhs == null)
			return false;
		if (!this.getClass().equals(rhs.getClass()))
			return false;
		
		return this.getIdentifier().equals(((PersistingElement)rhs).getIdentifier());
	}
	
	@Transient private int PersistingElement.hashCode = -1;

	@Continuator
	public int PersistingElement.hashCode() {
		if (this.hashCode == -1) {
			this.hashCode = this.getFullIdentifier().hashCode();
		}
		return this.hashCode;
	}
	
	public int PersistingElement.compareTo(PersistingElement rhs) {
		if (rhs == null)
			return 1;
		int clsCmp = this.getClass().getName().compareTo(rhs.getClass().getName());
		if (clsCmp != 0)
			return clsCmp;
		
		return this.getIdentifier().compareTo(((PersistingElement)rhs).getIdentifier());
	}
	
	public String identifierToString(String ident) {
		if (ident == null)
			return "unset";

		ident = ident.replace(KeyManagement.KEY_SEPARATOR, ":");
		ident = ident.replace(KeyManagement.KEY_END_SEPARATOR, "}");
		ident = ident.replace(KeyManagement.ARRAY_SEPARATOR, ";");
		
		return ident;
	}
	
	private transient String PersistingElement.stringRep = null;
	public String PersistingElement.toString() {
		if (this.stringRep != null ) {
			return this.stringRep;
		}
		
		String ret;
		StringBuffer sb = new StringBuffer();
		sb.append(this.getClass().getName());
		String ident = this.getIdentifier();
		if (ident == null) {
			sb.append(" with no key yet (missing some key values)");
			ret = sb.toString();
		} else {
			sb.append(" with key " + PersistingMixin.getInstance().identifierToString(ident));
			ret = sb.toString();
			this.stringRep = ret;
		}
		
		return ret;
	}
	
}
