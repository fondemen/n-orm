package com.googlecode.n_orm;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


public aspect IndexManagement {

	public static final String DECLARATION_SEPARATOR = "#";
	public static final String DECLARATION_REVERTED_KEYWORD = "reverted";
	public static final String DECLARATION_REVERTED_POSTFIX = DECLARATION_SEPARATOR + DECLARATION_REVERTED_KEYWORD;
	
	private static IndexManagement INSTANCE;
	
	public static IndexManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}

	//Similar declarations in KeyManagement
	declare error: set(@Index double PersistingElement+.*) : "Floating values not supported in indexes...";
	declare error: set(@Index java.lang.Double PersistingElement+.*) : "Floating values not supported in indexes...";
	declare error: set(@Index float PersistingElement+.*) : "Floating values not supported in indexes...";
	declare error: set(@Index java.lang.Float PersistingElement+.*) : "Floating values not supported in indexes...";

	declare error: set(@Index final * *.*) : "An index should not be final";
	

	private Map<Class<? extends PersistingElement>, Set<IndexDeclaration>> typeIndexes = new HashMap<Class<? extends PersistingElement>, Set<IndexDeclaration>>();
	/**
	 * Indexes explicitly declared on this clazz thanks to the {@link Indexable} annotation.
	 * Results are cached forever.
	 */
	public Set<IndexDeclaration> getIndexDeclarations(Class<? extends PersistingElement> clazz) {
		Set<IndexDeclaration> ret = this.typeIndexes.get(clazz);
		
		if (ret == null) {
			ret = new TreeSet<IndexDeclaration>();
			Indexable ann = clazz.getAnnotation(Indexable.class);
			if (ann != null) {
				for (String indexName : ann.value()) {
					ret.add(new IndexDeclaration(clazz, indexName));
				}
				typeIndexes.put(clazz, ret);
			}
		}
		
		return ret;
	}
	
	private transient Map<IndexDeclaration, String> PersistingElement.indexIdentifiers = null;
	private transient volatile boolean PersistingElement.creatingIndexIdentifiers = false;
	
	private void PersistingElement.createIdentifiers() {
		if (this.indexIdentifiers == null) {
			synchronized(this) {
				if (this.indexIdentifiers == null) {
					if (this.creatingIndexIdentifiers)
						return;
					this.creatingIndexIdentifiers = true;
					try {
						KeyManagement km = KeyManagement.getInstance();
						IndexManagement im = IndexManagement.getInstance();
						
						Map<IndexDeclaration, String> ii = new TreeMap<IndexDeclaration, String>();
						
						Class<? extends PersistingElement> clazz = this.getClass().asSubclass(PersistingElement.class);
						do {
							for(IndexDeclaration i : im.getIndexDeclarations(clazz)) {
								this.indexIdentifiers.put(i, km.createIdentifier(this, i.getClass(), i));
							}
							
							try {
								clazz = clazz.getSuperclass().asSubclass(PersistingElement.class);
							} catch (ClassCastException x) {
								break;
							}
						} while (clazz != null);
						
						this.indexIdentifiers = Collections.unmodifiableMap(ii);
					} finally {
						this.creatingIndexIdentifiers = false;
					}
				}
			}
		}
	}
	
	Map<IndexDeclaration, String> PersistingElement.getIndexIdentifiers() {
		this.createIdentifiers();
		return this.indexIdentifiers;
	}
	
	public Set<IndexDeclaration> PersistingElement.getIndexes() {
		this.createIdentifiers();
		return this.indexIdentifiers.keySet();
	}
	
	public String PersistingElement.getIdentifierForIndex(IndexDeclaration index) {
		this.createIdentifiers();
		return this.indexIdentifiers.get(index);
	}
}
