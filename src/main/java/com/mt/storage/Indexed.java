package com.mt.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;
/**
 * This annotation is to be set on a {@link Set} column family.
 * It must provide a field with the {@link #field()} tagged value to state how the contained elements are to be indexed in the column family.
 * Indeed, by nature, a column family is a map between an element and a key (a qualifier in the HBase dialect) ; the Indexed annotation states which one of the properties (including keys) of the contained element is to be its key in the context of this column family.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Indexed {

	/**
	 * The name of the property (possibly a key) to be used as the key of the element to be contained in this set.
	 * One may see this property as the hascode of the element.
	 * Properties may be attributes of the element's class or a readable property following the Java beans style.
	 */
	String field();

}
