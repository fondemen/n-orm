package com.googlecode.n_orm;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


public aspect SecondaryKeyManagement {

	public static final String DECLARATION_SEPARATOR = "#";
	public static final String DECLARATION_REVERTED_KEYWORD = "reverted";
	public static final String DECLARATION_REVERTED_POSTFIX = DECLARATION_SEPARATOR + DECLARATION_REVERTED_KEYWORD;
	
	private static SecondaryKeyManagement INSTANCE;
	
	public static SecondaryKeyManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}

	//Similar declarations in KeyManagement
	declare error: set(@SecondaryKey double PersistingElement+.*) : "Floating values not supported in secondary keys...";
	declare error: set(@SecondaryKey java.lang.Double PersistingElement+.*) : "Floating values not supported in secondary keys...";
	declare error: set(@SecondaryKey float PersistingElement+.*) : "Floating values not supported in secondary keys...";
	declare error: set(@SecondaryKey java.lang.Float PersistingElement+.*) : "Floating values not supported in secondary keys...";

	declare error: set(@SecondaryKey final * *.*) : "A secondary key should not be final";
	

	private Map<Class<? extends PersistingElement>, Set<SecondaryKeyDeclaration>> typeSecondaryKeys = new HashMap<Class<? extends PersistingElement>, Set<SecondaryKeyDeclaration>>();
	/**
	 * Indexes explicitly declared on this clazz thanks to the {@link Indexable} annotation.
	 * Results are cached forever.
	 */
	public Set<SecondaryKeyDeclaration> getSecondaryKeyDeclarations(Class<? extends PersistingElement> clazz) {
		Set<SecondaryKeyDeclaration> ret = this.typeSecondaryKeys.get(clazz);
		
		if (ret == null) {
			ret = new TreeSet<SecondaryKeyDeclaration>();
			SecondaryKeys ann = clazz.getAnnotation(SecondaryKeys.class);
			if (ann != null) {
				for (String indexName : ann.value()) {
					ret.add(new SecondaryKeyDeclaration(clazz, indexName));
				}
			}
			typeSecondaryKeys.put(clazz, ret);
		}
		
		return ret;
	}
	
	private transient Map<SecondaryKeyDeclaration, String> PersistingElement.skIdentifiers = null;
	private transient volatile boolean PersistingElement.creatingSKIdentifiers = false;
	
	private void PersistingElement.createIdentifiers() {
		if (this.skIdentifiers == null && !this.creatingSKIdentifiers) {
			this.creatingSKIdentifiers = true;
			try {
				KeyManagement km = KeyManagement.getInstance();
				SecondaryKeyManagement skm = SecondaryKeyManagement.getInstance();
				
				Map<SecondaryKeyDeclaration, String> ski = new TreeMap<SecondaryKeyDeclaration, String>();
				Class<? extends PersistingElement> cls = this.getClass().asSubclass(PersistingElement.class);
				do {
					for(SecondaryKeyDeclaration sk : skm.getSecondaryKeyDeclarations(cls)) {
						ski.put(sk, km.createIdentifier(this, sk.getDeclaringClass(), sk, false));
					}
					
					try {
						cls = cls.getSuperclass().asSubclass(PersistingElement.class);
					} catch (ClassCastException x) {
						cls = null;
					}
				} while (cls != null);
				
				this.skIdentifiers = Collections.unmodifiableMap(ski);
			} finally {
				this.creatingSKIdentifiers = false;
			}
		}
	}
	
	before(PersistingElement self) throws IllegalStateException : execution(void PersistingElement.checkIsValid() throws IllegalStateException) && target(self) {
		self.createIdentifiers();
		KeyManagement km = KeyManagement.getInstance();
		for (Entry<SecondaryKeyDeclaration, String> id : self.skIdentifiers.entrySet()) {
			SecondaryKeyDeclaration sk = id.getKey();
			String newId = km.createIdentifier(self, sk.getDeclaringClass(), sk, true);
			if (! id.getValue().equals(newId))
				throw new IllegalStateException(sk + " has changed value for " + self + ": was " + PersistingMixin.toReadableString(id.getValue()) + " it became " + PersistingMixin.toReadableString(newId));
		}
	}
	
	Map<SecondaryKeyDeclaration, String> PersistingElement.getSecondaryKeyIdentifiers() {
		this.createIdentifiers();
		return this.skIdentifiers;
	}
	
	public Set<SecondaryKeyDeclaration> PersistingElement.getSecondaryKeys() {
		this.createIdentifiers();
		return this.skIdentifiers.keySet();
	}
	
	public String PersistingElement.getIdentifierForSecondaryKey(SecondaryKeyDeclaration secondaryKey) {
		this.createIdentifiers();
		return this.skIdentifiers == null ? null : this.skIdentifiers.get(secondaryKey);
	}
	
	public String PersistingElement.getIdentifierForSecondaryKey(Class<? extends PersistingElement> className, String secondaryKeyName) {
		this.createIdentifiers();
		secondaryKeyName = secondaryKeyName.trim();
		for (Entry<SecondaryKeyDeclaration, String> skid : this.skIdentifiers.entrySet()) {
			if (skid.getKey().getName().equalsIgnoreCase(secondaryKeyName) && skid.getKey().getDeclaringClass().equals(className))
				return skid.getValue();
		}
		throw new IllegalArgumentException("Unknown secondary key " + secondaryKeyName + " for " + this);
	}
}
