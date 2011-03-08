package com.mt.storage.query;


import com.mt.storage.PersistingElement;

public class ConstraintBuilder {
	
	public <T extends PersistingElement> ClassConstraintBuilder<T> ofClass(Class<T> clazz) {
		return new ClassConstraintBuilder<T>(clazz);
	}

}
