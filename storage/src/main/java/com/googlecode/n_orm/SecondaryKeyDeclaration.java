package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecondaryKeyDeclaration implements FieldsetHandler, Comparable<SecondaryKeyDeclaration> {
	public class SecondaryKeyField {
		private final Field field;
		private final int order;
		private final boolean reverted;
		
		public SecondaryKeyField(Field field, int order, boolean reverted) {
			super();
			this.field = field;
			this.order = order;
			this.reverted = reverted;
		}
		
		public SecondaryKeyDeclaration getSecondaryKeyDeclaration() {
			return SecondaryKeyDeclaration.this;
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

	private final Class<? extends PersistingElement> declaringClass;
	private final String name;
	private final List<SecondaryKeyField> indexes;
	private final Map<Field, SecondaryKeyField> indexesMap;
	private final List<Field> orderedFields;
	private String table = null;
	
	protected SecondaryKeyDeclaration(Class<? extends PersistingElement> declaringClass,
			String name) {
		assert declaringClass != null;
		assert name != null;
		if (name.length() == 0)
			throw new IllegalArgumentException(declaringClass.getName() + ": An secondary key name must not be empty");
		this.declaringClass = declaringClass;
		this.name = name;
		SecondaryKeyField[] detected = this.detectSecondaryKeys();
		this.indexes = Arrays.asList(detected);
		Map<Field, SecondaryKeyDeclaration.SecondaryKeyField> indexesMap = new HashMap<Field, SecondaryKeyDeclaration.SecondaryKeyField>();
		List<Field> orderedFields = new ArrayList<Field>(detected.length);
		for (SecondaryKeyField i : detected) {
			indexesMap.put(i.getField(), i);
			orderedFields.add(i.getField());
		}
		this.indexesMap = Collections.unmodifiableMap(indexesMap);
		this.orderedFields = Collections.unmodifiableList(orderedFields);
	}

	/**
	 * Detects field indexes for this index
	 */
	private SecondaryKeyField[] detectSecondaryKeys() {
		Class<?> clazz = this.declaringClass;
		ArrayList<SecondaryKeyField> detected = new ArrayList<SecondaryKeyField>(10);
		
		//Iterating over declaring classes and its superclasses
		do {
			for (Field f : clazz.getDeclaredFields()) {
				SecondaryKey fi = f.getAnnotation(SecondaryKey.class);
				if (fi != null) {
					for (String ids : fi.value()) {
						if (ids.startsWith(this.name)) {
							String pids = ids.substring(this.name.length());
							
							//Detecting whether this index declaration is reverted
							boolean reverted = pids.toLowerCase().endsWith(SecondaryKeyManagement.DECLARATION_REVERTED_POSTFIX);
							if (reverted)
								pids = pids.substring(0, pids.length() - SecondaryKeyManagement.DECLARATION_REVERTED_POSTFIX.length());
							
							//Detecting order
							int order;
							if (pids.length() == 0)
								order = 1;
							else if (pids.startsWith(SecondaryKeyManagement.DECLARATION_SEPARATOR)) {
								pids = pids.substring(SecondaryKeyManagement.DECLARATION_SEPARATOR.length());
								try {
									order = Integer.parseInt(pids);
								} catch (NumberFormatException e) {
									throw new IllegalArgumentException(ids + ": malformed index declaration for " + f + ": pattern should be NAME('" + SecondaryKeyManagement.DECLARATION_SEPARATOR + "'order)?('" + SecondaryKeyManagement.DECLARATION_SEPARATOR + "'" + SecondaryKeyManagement.DECLARATION_REVERTED_KEYWORD + ")?", e);
								}
							} else {
								throw new IllegalArgumentException(ids + ": malformed index declaration for " + f + ": pattern should be NAME('" + SecondaryKeyManagement.DECLARATION_SEPARATOR + "'order)?('" + SecondaryKeyManagement.DECLARATION_SEPARATOR + "'" + SecondaryKeyManagement.DECLARATION_REVERTED_KEYWORD + ")?");
							}
							
							//Ensuring that the detected list is large enough for this order
							while(detected.size() < order)
								detected.add(null);
							
							//In case an index already exists, there is a conflict
							SecondaryKeyField alreadyDeclared = detected.get(order-1);
							if (alreadyDeclared != null) {
								assert alreadyDeclared.getSecondaryKeyDeclaration().getName().equals(this.getName());
								assert alreadyDeclared.getSecondaryKeyDeclaration() == this;
								assert alreadyDeclared.getOrder() == order;
								assert alreadyDeclared.getField() != f;
								throw new IllegalArgumentException("Index " + this.name + " declares two field for same order " + f + " and " + alreadyDeclared.getField());
							}

							detected.set(order-1,new SecondaryKeyField(f, order, reverted));
						}
					}
				}
			}
			
			clazz = clazz.getSuperclass();
		} while(clazz != null);
		
		if (detected.isEmpty())
			throw new IllegalArgumentException("No field for secondary key " + this.name + " on class " + this.declaringClass.getName() + " ; use annotation @" + SecondaryKey.class.getName() + "({\"" + this.name + "\"}) on a field of the class or of one of its superclasss");
		
		SecondaryKeyField[] ret = detected.toArray(new SecondaryKeyField[detected.size()]);
		
		//Checking that all orders are there
		for (int i = 0; i < ret.length; i++) {
			SecondaryKeyField dif = ret[i];
			int order = i+1;
			
			if (dif == null)
				throw new IllegalArgumentException("Missing field of order " + order + " for secondary key " + this.name);

			assert dif.getSecondaryKeyDeclaration().getName().equals(this.getName());
			assert dif.getSecondaryKeyDeclaration() == this;
			assert dif.getOrder() == order;
			assert dif.getField() != null;
		}
		
		return ret;
	}

	public Class<? extends PersistingElement> getDeclaringClass() {
		return declaringClass;
	}

	public String getName() {
		return name;
	}

	public List<SecondaryKeyField> getIndexes() {
		return this.indexes;
	}

	@Override
	public int compareTo(SecondaryKeyDeclaration o) {
		if (this == o)
			return 0;
		
		return (this.declaringClass == o.declaringClass ? 0 : this.declaringClass.getName().compareTo(o.declaringClass.getName()))
				+ this.name.compareTo(o.name);
	}

	@Override
	public String getIdentifier(PersistingElement elt) {
		return elt.getIdentifierForSecondaryKey(this);
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
		return this.name + " secondary key";
	}
	
	public String getTable() {
		if (this.table == null) {
			this.table = PersistingMixin.getInstance().getTable(this.getDeclaringClass()) + '.' + this.getName();
		}
		return this.table;
	}

	@Override
	public String toString() {
		return "secondary key " + this.getDeclaringClass().getName() + '.' + this.getName();
	}
	
}
