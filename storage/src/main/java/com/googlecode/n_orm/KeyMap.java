package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used for mapping constructor's parameters to key values.
 * Its is often necessary to {@link StorageManagement#getElementWithKeys(Class, Object...) build a new object for its key values},
 * for example when {@link StorageManagement#findElements() searching} or {@link PersistingElement#activate(String...) activating} a property whose type is an object.
 * To do so, by default, the default constructor is used, then values for is {@link Key keys} are set.
 * It is also possible to define the constructor to be used by explicitly stating which parameter correspond to which key.
 * For instance:<pre>
 * {@literal @}Persisting class Storable {
 * 	{@literal @}Key(order=1) public String key1;
 * 	{@literal @}Key(order=2) public String key2;
 * 
 * 	public Storable ({@literal @}KeyMap("key2") String val2, {@literal @}KeyMap("key1") String val1) {
 * 		this.key1 = val1;
 * 		this.key2 = val2;
 * 	}
 * }</pre>
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface KeyMap {
	String value();
}
