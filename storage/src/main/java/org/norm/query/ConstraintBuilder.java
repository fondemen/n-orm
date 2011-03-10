package org.norm.query;


import org.norm.PersistingElement;

public class ConstraintBuilder {
	
	public <T extends PersistingElement> ClassConstraintBuilder<T> ofClass(Class<T> clazz) {
		return new ClassConstraintBuilder<T>(clazz);
	}

}
