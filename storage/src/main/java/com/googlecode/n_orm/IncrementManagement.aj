package com.googlecode.n_orm;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.aspectj.lang.reflect.FieldSignature;

import com.googlecode.n_orm.IncrementException;
import com.googlecode.n_orm.IncrementManagement;
import com.googlecode.n_orm.Incrementing;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.Incrementing.Mode;
import com.googlecode.n_orm.PropertyManagement.Property;
import com.googlecode.n_orm.PropertyManagement.PropertyFamily;
import com.googlecode.n_orm.cf.MapColumnFamily;


public aspect IncrementManagement {
	private static IncrementManagement INSTANCE;
	
	public static IncrementManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}
	
	private final PropertyManagement propertyManager = PropertyManagement.getInstance();

	declare error: set(@Incrementing (!long && !int && !short && !byte
										&& !MapColumnFamily && !Map) *.*)
			: "Only naturals or maps of naturals may be incremented";
	declare error: set(@Incrementing @Key * *.*): "Keys (that must be final) cannot be incremented";
	declare error: set(@Incrementing * (!PersistingElement+).*) : "Incrementing properties must appear in persisting classes";
	
	private transient Map<String, Number> PersistingElement.increments = new HashMap<String, Number>();
	
	Map<String, Number> PersistingElement.getIncrements() {
		return this.increments;
	}

	public Number getActualIncrement(Number val,
			Number oldVal, Number previousIncrement, Field prop) throws IncrementException {
		long value = toLong(val, prop);
		long oldValue = toLong(oldVal, prop);
		Incrementing inca = prop.getAnnotation(Incrementing.class);
		if (inca != null) {
			if (inca.mode().equals(Mode.IncrementOnly) && oldValue > value)
				throw new IncrementException(prop, false, oldValue-value);

			if (inca.mode().equals(Mode.DecrementOnly) && oldValue < value)
				throw new IncrementException(prop, true, value-oldValue);
		}
		Number increment = previousIncrement;
		if (increment == null)
			increment = 0l;
		long inc = increment.longValue()+value-oldValue;
		return toNumber(((Number)val).getClass(), inc);
	}

	protected long toLong(Object val, Field prop) {
		long value;
		if (val == null)
			value = 0;
		else if (val instanceof Long)
			value = (Long) val;
		else if (val instanceof Integer)
			value = (Integer) val;
		else if (val instanceof Short)
			value = (Short) val;
		else if (val instanceof Byte)
			value = (Byte) val;
		else if (prop != null)
			throw new IllegalArgumentException("Cannot annotate non natural field " + prop + " with " + Incrementing.class);
		else
			value = 0;
		return value;
	}
	
	protected Number toNumber(Class<? extends Number> clazz, long l) {
		if (clazz.equals(Byte.class) || clazz.equals(byte.class))
			return (byte)l;
		else if (clazz.equals(Short.class) || clazz.equals(short.class))
			return (short)l;
		else if (clazz.equals(Integer.class) || clazz.equals(int.class))
			return (int)l;
		else
			return l;
	}
	
//	public void checkIncrementable(Class<?> clazz) {
//		if (! (long.class.equals(clazz) || int.class.equals(clazz) || short.class.equals(clazz) || byte.class.equals(clazz) 
//			|| Long.class.equals(clazz) || Integer.class.equals(clazz) || Short.class.equals(clazz) || Byte.class.equals(clazz)))
//			throw new IllegalArgumentException(clazz + " is not a natural type such as int.");
//	}
	
	//@Override
	public void PropertyFamily.clearChanges() {
		super.clearChanges();
		this.getOwner().getIncrements().clear();
	}
}
