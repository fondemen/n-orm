package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.List;

interface FieldsetHandler {

	String getIdentifier(PersistingElement elt);
	List<Field> getFields(Class<?> clazz);
	boolean isReverted(Field f);
	String handledFieldKind();
}
