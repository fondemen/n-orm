package org.norm;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.aspectj.lang.reflect.FieldSignature;
import org.norm.PropertyManagement.Property;
import org.norm.PropertyManagement.PropertyFamily;
import org.norm.cf.MapColumnFamily;


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

//	after(PersistingElement self) returning : PersistingMixin.creation(self) {
//		PropertyFamily pf = self.getProperties();
//		for (Field f : PropertyManagement.aspectOf().getProperties(self.getClass())) {
//			if (f.isAnnotationPresent(Incrementing.class) && !self.getIncrements().containsKey(f.getName())) {
//				self.getIncrements().put(f.getName(), 0);
//			}
//			
//		}
//	}
	
	after(PersistingElement owner) returning: execution(void org.norm.PersistingElement+.upgradeProperties()) && target(owner) {
		PropertyFamily propertyFam = owner.getProperties();
		for (Field f : propertyManager.getProperties(owner.getClass())) {
			if (f.isAnnotationPresent(Incrementing.class)) {
				Property prop = propertyFam.getElement(f.getName());
				if (prop != null) {
					if (prop.getField() == null)
						prop.setField(f);
					assert f.equals(prop.getField());
					propertyManager.candideSetValue(owner, f, prop.getValue());
				}
			}
		}
	}
	
	before(PersistingElement self, Object val): PropertyManagement.attUpdated(self, val) && set(@org.norm.Incrementing * *.*) {
		Field prop = ((FieldSignature)thisJoinPointStaticPart.getSignature()).getField();
		Number oldVal = (Number) propertyManager.candideReadValue(self, prop);
		self.getIncrements().put(prop.getName(), getActualIncrement((Number)val, oldVal, self.getIncrements().get(prop.getName()), prop));
	}

	public Number getActualIncrement(Number val,
			Number oldVal, Number previousIncrement, Field prop) throws DecrementException {
		long value = toLong(val, prop);
		long oldValue = toLong(oldVal, prop);
		if (oldValue > value)
			throw new DecrementException("Property " + prop + " can only increase with time ; trying to decrement it of " + (oldValue-value));
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
