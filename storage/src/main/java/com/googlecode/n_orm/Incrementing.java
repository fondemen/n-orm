package com.googlecode.n_orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

import com.googlecode.n_orm.IncrementException;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PersistingElement;
/**
 * This annotation may be placed either on a number property, or on a {@link Map} of numbers.
 * By default, the property is only allowed to increment over time.
 * For maps, it's the element at a given key that must not decrement.
 * Regarding storage, it's not the actual value that is sent when invoking {@link PersistingElement#store()}, but the difference.
 * The situation is illustrated by the following example:<br><code>
 * &#64;{@link Persisting} public class MyClass {&#64;Incrementing public int anInt;}<br>
 * <br>
 * MyClass mc1 = new MyClass(); mc1.anInt = 3; mc1.store();<br>
 * MyClass mc2 = new MyClass(); mc2.anInt = 2; mc2.store();<br>
 * MyClass mc3 = new MyClass(); mc3.activate(); assert mc3.anInt == 5;
 * </code>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Incrementing {
	
	public enum Mode {Free, IncrementOnly, DecrementOnly};
	
	/**
	 * Whether the incrementing property is allowed to increment or decrement over time.
	 * For non free modes, a {@link IncrementException} is thrown if constraint set by mode is not enforced.
	 * As there is no mean to state a default value, {@link Mode#IncrementOnly} properties must be positive and {@link Mode#IncrementOnly} must be negative.
	 */
	Mode mode() default Mode.IncrementOnly;
}
