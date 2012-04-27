package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexDeclaration implements FieldsetHandler, Comparable<IndexDeclaration> {
	public class IndexField {
		private final Field field;
		private final int order;
		private final boolean reverted;
		
		public IndexField(Field field, int order, boolean reverted) {
			super();
			this.field = field;
			this.order = order;
			this.reverted = reverted;
		}
		
		public IndexDeclaration getIndexDeclaration() {
			return IndexDeclaration.this;
		}

		public Field getField() {
			return field;
		}

		public int getOrder() {
			return order;
		}

		public boolean isReverted() {
			return reverted;
		};
		
	}

	private final Class<?> declaringClass;
	private final String name;
	private final List<IndexField> indexes;
	private final Map<Field, IndexField> indexesMap;
	private final List<Field> orderedFields;
	
	protected IndexDeclaration(Class<?> declaringClass,
			String name) {
		assert declaringClass != null;
		assert name != null;
		if (name.length() == 0)
			throw new IllegalArgumentException(declaringClass.getName() + ": An index name must not be empty");
		this.declaringClass = declaringClass;
		this.name = name;
		IndexField[] detected = this.detectIndexes();
		this.indexes = Arrays.asList(detected);
		Map<Field, IndexDeclaration.IndexField> indexesMap = new HashMap<Field, IndexDeclaration.IndexField>();
		List<Field> orderedFields = new ArrayList<Field>(detected.length);
		for (IndexField i : detected) {
			indexesMap.put(i.getField(), i);
			orderedFields.add(i.getField());
		}
		this.indexesMap = Collections.unmodifiableMap(indexesMap);
		this.orderedFields = Collections.unmodifiableList(orderedFields);
	}

	/**
	 * Detects field indexes for this index
	 */
	private IndexField[] detectIndexes() {
		Class<?> clazz = this.declaringClass;
		ArrayList<IndexField> detected = new ArrayList<IndexField>(10);
		
		//Iterating over declaring classes and its superclasses
		do {
			for (Field f : clazz.getDeclaredFields()) {
				Index fi = f.getAnnotation(Index.class);
				if (fi != null) {
					for (String ids : fi.value()) {
						if (ids.startsWith(this.name)) {
							String pids = ids.substring(this.name.length());
							
							//Detecting whether this index declaration is reverted
							boolean reverted = pids.toLowerCase().endsWith(IndexManagement.DECLARATION_REVERTED_POSTFIX);
							if (reverted)
								pids = pids.substring(0, pids.length() - IndexManagement.DECLARATION_REVERTED_POSTFIX.length());
							
							//Detecting order
							int order;
							if (pids.length() == 0)
								order = 1;
							else if (pids.startsWith(IndexManagement.DECLARATION_SEPARATOR)) {
								pids = pids.substring(IndexManagement.DECLARATION_SEPARATOR.length());
								try {
									order = Integer.parseInt(pids);
								} catch (NumberFormatException e) {
									throw new IllegalArgumentException(ids + ": malformed index declaration for " + f + ": pattern should be NAME('" + IndexManagement.DECLARATION_SEPARATOR + "'order)?('" + IndexManagement.DECLARATION_SEPARATOR + "'" + IndexManagement.DECLARATION_REVERTED_KEYWORD + ")?", e);
								}
							} else {
								throw new IllegalArgumentException(ids + ": malformed index declaration for " + f + ": pattern should be NAME('" + IndexManagement.DECLARATION_SEPARATOR + "'order)?('" + IndexManagement.DECLARATION_SEPARATOR + "'" + IndexManagement.DECLARATION_REVERTED_KEYWORD + ")?");
							}
							
							//Ensuring that the detected list is large enough for this order
							while(detected.size() < order)
								detected.add(null);
							
							//In case an index already exists, there is a conflict
							IndexField alreadyDeclared = detected.get(order-1);
							if (alreadyDeclared != null) {
								assert alreadyDeclared.getIndexDeclaration().getName().equals(this.getName());
								assert alreadyDeclared.getIndexDeclaration() == this;
								assert alreadyDeclared.getOrder() == order;
								assert alreadyDeclared.getField() != f;
								throw new IllegalArgumentException("Index " + this.name + " declares two field for same order " + f + " and " + alreadyDeclared.getField());
							}

							detected.set(order-1,new IndexField(f, order, reverted));
						}
					}
				}
			}
			
			clazz = clazz.getSuperclass();
		} while(clazz != null);
		
		if (detected.isEmpty())
			throw new IllegalArgumentException("No field for index " + this.name + " on class " + this.declaringClass.getName() + " ; use annotation @" + Index.class.getName() + "({\"" + this.name + "\"}) on a field of the class or of one of its superclasss");
		
		IndexField[] ret = detected.toArray(new IndexField[detected.size()]);
		
		//Checking that all orders are there
		for (int i = 0; i < ret.length; i++) {
			IndexField dif = ret[i];
			int order = i+1;
			
			if (dif == null)
				throw new IllegalArgumentException("Missing field of order " + order + " for index " + this.name);

			assert dif.getIndexDeclaration().getName().equals(this.getName());
			assert dif.getIndexDeclaration() == this;
			assert dif.getOrder() == order;
			assert dif.getField() != null;
		}
		
		return ret;
	}

	public Class<?> getDeclaringClass() {
		return declaringClass;
	}

	public String getName() {
		return name;
	}

	public List<IndexField> getIndexes() {
		return this.indexes;
	}

	@Override
	public int compareTo(IndexDeclaration o) {
		if (this == o)
			return 0;
		
		return (this.declaringClass == o.declaringClass ? 0 : this.declaringClass.getName().compareTo(o.declaringClass.getName()))
				+ this.name.compareTo(o.name);
	}

	@Override
	public String getIdentifier(PersistingElement elt) {
		return elt.getIdentifierForIndex(this);
	}

	@Override
	public List<Field> getFields(Class<?> clazz) {
		return this.orderedFields;
	}

	@Override
	public boolean isReverted(Field f) {
		return this.indexesMap.get(f).isReverted();
	}

	@Override
	public String handledFieldKind() {
		return this.name + " index";
	}
	
}
