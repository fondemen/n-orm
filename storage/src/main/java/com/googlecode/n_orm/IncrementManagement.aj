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
import com.googlecode.n_orm.PropertyManagement.PropertyFamily;
import com.googlecode.n_orm.cf.MapColumnFamily;


public aspect IncrementManagement {
	private static IncrementManagement INSTANCE;

	public static IncrementManagement getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}
	
	declare error: set(@Incrementing (!long && !int && !short && !byte
										&& !MapColumnFamily && !Map) *.*)
			: "Only naturals or maps of naturals may be incremented";
	declare warning: set(@Incrementing @Key * *.*): "Keys (that identify a persisting element) should not be incremented";
	
	private static volatile boolean immedatePropertyCheck = true;
	
	/**
	 * Whether setting a property marked as {@link Incrementing} immediately triggers an check that might throw an {@link IncrementException}.
	 */
	public static boolean isImmedatePropertyCheck() {
		return immedatePropertyCheck;
	}

	/**
	 * Whether setting a property marked as {@link Incrementing} immediately triggers an check that might throw an {@link IncrementException}.
	 */
	public static void setImmedatePropertyCheck(boolean _immedatePropertyCheck) {
		immedatePropertyCheck = _immedatePropertyCheck;
	}
	
	private transient Map<String, Number> PersistingElement.increments;
	
	Map<String, Number> PersistingElement.getIncrements() {
		// increments is transient, it should be initialized somewhere
		if(this.increments == null)
		{
			 this.increments = new HashMap<String, Number>();
		}
		return this.increments;
	}

	public Number getActualIncrement(Number val,
			Number oldVal, Number previousIncrement, Field prop) throws IncrementException {
		long value = toLong(val, prop);
		long oldValue = toLong(oldVal, prop);
		checkIncrement(prop, value, oldValue);
		Number increment = previousIncrement;
		if (increment == null)
			increment = 0l;
		long inc = increment.longValue()+value-oldValue;
		return toNumber(((Number)val).getClass(), inc);
	}

	private void checkIncrement(Field prop, long value, long oldValue) {
		Incrementing inca = prop.getAnnotation(Incrementing.class);
		if (inca != null) {
			if (inca.mode().equals(Mode.IncrementOnly) && oldValue > value)
				throw new IncrementException(prop, true, oldValue-value);

			if (inca.mode().equals(Mode.DecrementOnly) && oldValue < value)
				throw new IncrementException(prop, false, value-oldValue);
		}
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
	
	//@Override
	public void PropertyFamily.clearChanges() {
		super.clearChanges();
		this.getOwner().getIncrements().clear();
	}
	
	before(PersistingElement self, Object newValue): set(@Incrementing (Number+||long||int||short||byte) PersistingElement+.*) && target(self) && args(newValue) && if(immedatePropertyCheck) {
		Field f = ((FieldSignature)thisJoinPointStaticPart.getSignature()).getField();
		Incrementing inc = f.getAnnotation(Incrementing.class);
		if (Mode.Free.equals(inc.mode()))
			return;
		this.checkIncrement(f, toLong((Number)newValue, f), toLong((Number)PropertyManagement.getInstance().candideReadValue(self, f), f));
	}
}
