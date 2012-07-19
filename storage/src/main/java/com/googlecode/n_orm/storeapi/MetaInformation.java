package com.googlecode.n_orm.storeapi;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;

/**
 * Simple unchecked class to be sent to data stores for additional and optional
 * information regarding queries.
 */
public class MetaInformation {

	// Adding a field here should alter the cloning constructor, equals and
	// hascode methods.
	private Class<? extends PersistingElement> clazz;
	private Map<String, Field> families;
	private Field property;
	private PersistingElement element;
	private String tablePostfix;

	public MetaInformation() {
	}

	public MetaInformation(MetaInformation clone) {
		this.clazz = clone.clazz;
		this.families = clone.families;
		this.property = clone.property;
		this.element = clone.element;
		this.tablePostfix = clone.tablePostfix;
	}

	public MetaInformation forClass(Class<? extends PersistingElement> clazz) {
		this.clazz = clazz;
		return this;
	}

	public MetaInformation forElement(PersistingElement element) {
		if (this.clazz == null)
			this.clazz = element.getClass();
		this.element = element;
		return this;
	}

	public MetaInformation forProperty(Field property) {
		this.property = property;
		return this;
	}

	public MetaInformation withColumnFamilies(Map<String, Field> families) {
		this.families = families == null ? null : Collections.unmodifiableMap(families);
		return this;
	}

	public MetaInformation withPostfixedTable(String originalTable,
			String postfix) {
		this.tablePostfix = postfix;
		return this;
	}

	public Class<? extends PersistingElement> getClazz() {
		// A federated table should be with a postfix (at least "")
		assert (clazz != null && clazz.getAnnotation(Persisting.class).federated().isFederated()) == (tablePostfix != null);
		return clazz;
	}

	public Class<? extends PersistingElement> getClazzNoCheck() {
		return clazz;
	}

	public Map<String, Field> getFamilies() {
		return families;
	}

	public Field getProperty() {
		return property;
	}

	public PersistingElement getElement() {
		return element;
	}

	public String getTablePostfix() {
		return tablePostfix;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
		result = prime * result + ((element == null) ? 0 : element.hashCode());
		result = prime * result
				+ ((families == null) ? 0 : families.hashCode());
		result = prime * result
				+ ((property == null) ? 0 : property.hashCode());
		result = prime * result
				+ ((tablePostfix == null) ? 0 : tablePostfix.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MetaInformation other = (MetaInformation) obj;
		if (clazz == null) {
			if (other.clazz != null)
				return false;
		} else if (!clazz.equals(other.clazz))
			return false;
		if (element == null) {
			if (other.element != null)
				return false;
		} else if (!element.equals(other.element))
			return false;
		if (families == null) {
			if (other.families != null)
				return false;
		} else if (!families.equals(other.families))
			return false;
		if (property == null) {
			if (other.property != null)
				return false;
		} else if (!property.equals(other.property))
			return false;
		if (tablePostfix == null) {
			if (other.tablePostfix != null)
				return false;
		} else if (!tablePostfix.equals(other.tablePostfix))
			return false;
		return true;
	}

}
