package com.googlecode.n_orm.hbase;

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;

import com.googlecode.n_orm.PersistingElement;

public class PropertyUtils {
	public static final HColumnFamilyProperty[] properties;
	
	static {
		properties = new HColumnFamilyProperty[] {
			new InMemoryProperty(),
			new CompressorProperty()
		};
	}

	public static abstract class HBaseProperty<T> {
		
		abstract String getName();
		
		abstract T readValue(HBaseSchema ann); 
		
		abstract T getDefaultValue(Store store);
		
		abstract boolean isSet(T value);
	
		/**
		 * @return null if unset
		 */
		T getValue(Store store, Class<? extends PersistingElement> clazz, Field field) {
			HBaseSchema classLevelSchemaSpecificities = clazz == null ? null : clazz.getAnnotation(HBaseSchema.class);
			HBaseSchema columnFamilyLevelSchemaSpecificities = field == null ? null : field.getAnnotation(HBaseSchema.class);
			
			for (HBaseSchema schemaSpecificities : new HBaseSchema[] {columnFamilyLevelSchemaSpecificities, classLevelSchemaSpecificities}) {
				if (schemaSpecificities != null) {
					T ret = this.readValue(schemaSpecificities);
					if (this.isSet(ret))
						return ret;
				}
			}
			
			return this.getDefaultValue(store);
		}
		
		@Override
		public String toString() {
			return this.getName();
		}
	}

	public static abstract class HColumnFamilyProperty<T> extends HBaseProperty<T> {
		abstract boolean hasValue(T value, HColumnDescriptor cf);
		
		public boolean hasValue(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			return this.isSet(value) ? this.hasValue(value, cf) : true;
		}
		
		public abstract void setValue(T value, HColumnDescriptor cf);
		
		public void setValue(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			if (this.isSet(value)) {
				this.setValue(value, cf);
			}
		}
		
		public boolean alter(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			if (this.isSet(value) && !this.hasValue(value, cf)) {
				this.setValue(value, cf);
				return true;
			} else {
				return false;
			}
		}
		
	}

	private static abstract class ShouldForceHBaseProperty extends HBaseProperty<Boolean> {
		static Boolean getBoolean(HBaseSchema.SettableBoolean value) {
			switch (value) {
			case UNSET: return null;
			case FALSE: return Boolean.FALSE;
			case TRUE: return Boolean.TRUE;
			default: return null;
			}
		}
		
		@Override
		boolean isSet(Boolean value) {
			return value != null;
		}

		abstract HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann);
	
		@Override
		Boolean readValue(HBaseSchema ann) {
			return getBoolean(this.readRawValue(ann));
		}		
	}

	private static abstract class ForcableHBaseProperty<T> extends HColumnFamilyProperty<T> {
		private ShouldForceHBaseProperty forcedProperty;
		
		public ForcableHBaseProperty(ShouldForceHBaseProperty forcedProperty) {
			this.forcedProperty = forcedProperty;
		}
		
		@Override
		public boolean alter(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field) {
			Boolean shouldForce = this.forcedProperty.getValue(store, clazz, field);
			if (this.forcedProperty.isSet(shouldForce) && Boolean.TRUE.equals(shouldForce)) {
				return super.alter(cf, store, clazz, field);
			} else {
				return false;
			}
		}
	}

	private static class CompressorForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceCompression();
		}
		
		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceCompression();
		}

		@Override
		String getName() {
			return "force compression";
		}
	
	}

	private static class CompressorProperty extends ForcableHBaseProperty<Algorithm> {
		public CompressorProperty() {
			super(new CompressorForcedProperty());
		}
	
		@Override
		boolean hasValue(Algorithm value, HColumnDescriptor cf) {
			return cf.getCompressionType().equals(value);
		}
	
		@Override
		public void setValue(Algorithm value, HColumnDescriptor cf) {
			cf.setCompressionType(value);
		}
	
		@Override
		Algorithm readValue(HBaseSchema ann) {
			return Store.getCompressionByName(ann.compression());
		}
	
		@Override
		Algorithm getDefaultValue(Store store) {
			return store.getCompressionAlgorithm();
		}

		@Override
		boolean isSet(Algorithm value) {
			return value != null;
		}

		@Override
		String getName() {
			return "compression";
		}
	}

	private static class InMemoryForcedProperty extends ShouldForceHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceInMemory();
		}
		
		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceInMemory();
		}

		@Override
		String getName() {
			return "force inMemory";
		}
	
	}

	private static class InMemoryProperty extends ForcableHBaseProperty<Boolean> {
		public InMemoryProperty() {
			super(new InMemoryForcedProperty());
		}
	
		@Override
		boolean hasValue(Boolean value, HColumnDescriptor cf) {
			return cf.isInMemory() == value.booleanValue();
		}
	
		@Override
		public void setValue(Boolean value, HColumnDescriptor cf) {
			cf.setInMemory(value);
		}
	
		@Override
		Boolean readValue(HBaseSchema ann) {
			return ShouldForceHBaseProperty.getBoolean(ann.inMemory());
		}
	
		@Override
		Boolean getDefaultValue(Store store) {
			return store.isInMemory();
		}

		@Override
		boolean isSet(Boolean value) {
			return value != null;
		}

		@Override
		String getName() {
			return "inMemory";
		}
	}

}
