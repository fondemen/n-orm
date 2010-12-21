package com.mt.storage;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.aspectj.lang.reflect.FieldSignature;

public aspect IncrementManagement {


	declare error: set(@Incrementing (!long && !int && !short && !byte
										&& !Collection
										&& !ColumnFamily) *.*)
			: "Only naturals or collection of naturals may be incremented";
	declare error: set(@Incrementing @Key * *.*): "Keys (that must be final) cannot be incremented";
	declare error: set(@Incrementing * (!PersistingElement+).*) : "Incrementing properties must appear in persisting classes";
	
	private transient Map<String, Number> PersistingElement.increments = new HashMap<String, Number>();
	
	Map<String, Number> PersistingElement.getIncrements() {
		return this.increments;
	}
	
	before(PersistingElement self, Object val): PropertyManagement.attUpdated(self, val) && set(@Incrementing * *.*) {
		Field prop = ((FieldSignature)thisJoinPointStaticPart.getSignature()).getField();
		Number oldVal = (Number) PropertyManagement.aspectOf().candideReadValue(self, prop);
		self.getIncrements().put(prop.getName(), getActualIncrement((Number)val, oldVal, self.getIncrements().get(prop.getName()), prop));
	}

	protected Number getActualIncrement(Number val,
			Number oldVal, Number previousIncrement, Field prop) {
		long value = toLong(val, prop);
		long oldValue = toLong(oldVal, prop);
		if (oldValue > value)
			throw new IllegalArgumentException("Property " + prop + " can only increase with time.");
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
	
	public void checkIncrementable(Class<?> clazz) {
		if (! (long.class.equals(clazz) || int.class.equals(clazz) || short.class.equals(clazz) || byte.class.equals(clazz) 
			|| Long.class.equals(clazz) || Integer.class.equals(clazz) || Short.class.equals(clazz) || Byte.class.equals(clazz)))
			throw new IllegalArgumentException(clazz + " is not a natural type such as int.");
	}
	
	after(PropertyManagement.PropertyFamily pf) returning : execution(protected void PropertyManagement.PropertyFamily.activate()) && target(pf) {
		pf.getOwner().getIncrements().clear();
	}
	
	after(PersistingElement self) returning: execution(void PersistingElement+.store()) && target(self) {
		self.getIncrements().clear();
	}
	
	boolean around(PropertyManagement.PropertyFamily properties, PropertyManagement.Property property) : call(boolean Collection+.add(Object)) && within(PropertyManagement) &&  target(properties) && args(property) {
		Field f = property.getField();
		if (f != null && f.isAnnotationPresent(Incrementing.class))
			return false;
		else
			return proceed(properties, property);
	}
}
