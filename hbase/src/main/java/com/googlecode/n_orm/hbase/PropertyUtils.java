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
	}

	public static abstract class HColumnFamilyProperty<T> extends HBaseProperty<T> {
		abstract boolean hasValue(T value, HColumnDescriptor cf);
		
		public abstract void setValue(T value, HColumnDescriptor cf);
		
		public boolean alter(HColumnDescriptor cf, Store store, Class<? extends PersistingElement> clazz, Field field) {
			T value = this.getValue(store, clazz, field);
			if (this.isSet(value) && !hasValue(value, cf)) {
				this.setValue(value, cf);
				return true;
			} else {
				return false;
			}
		}
		
	}

	private static abstract class ForcingHBaseProperty extends HBaseProperty<Boolean> {
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
			return this.getBoolean(this.readRawValue(ann));
		}		
	}

	private static abstract class ForcableHBaseProperty<T> extends HColumnFamilyProperty<T> {
		private ForcingHBaseProperty forcedProperty;
		
		public ForcableHBaseProperty(ForcingHBaseProperty forcedProperty) {
			this.forcedProperty = forcedProperty;
		}
		
		@Override
		public boolean alter(HColumnDescriptor cf, Store store,
				Class<? extends PersistingElement> clazz, Field field) {
			Boolean shouldForce = this.forcedProperty.getValue(store, clazz, field);
			if (this.forcedProperty.isSet(shouldForce) && shouldForce == Boolean.TRUE) {
				return super.alter(cf, store, clazz, field);
			} else {
				return false;
			}
		}
	}

	private static class CompressorForcedProperty extends ForcingHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceCompression();
		}
		
		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceCompression();
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
	}

	private static class InMemoryForcedProperty extends ForcingHBaseProperty {
		@Override
		HBaseSchema.SettableBoolean readRawValue(HBaseSchema ann) {
			return ann.forceInMemory();
		}
		
		@Override
		Boolean getDefaultValue(Store store) {
			return store.isForceInMemory();
		}
	
	}

	private static class InMemoryProperty extends ForcableHBaseProperty<Boolean> {
		public InMemoryProperty() {
			super(new InMemoryForcedProperty());
		}
	
		@Override
		boolean hasValue(Boolean value, HColumnDescriptor cf) {
			return cf.isInMemory();
		}
	
		@Override
		public void setValue(Boolean value, HColumnDescriptor cf) {
			cf.setInMemory(value);
		}
	
		@Override
		Boolean readValue(HBaseSchema ann) {
			return ForcingHBaseProperty.getBoolean(ann.inMemory());
		}
	
		@Override
		Boolean getDefaultValue(Store store) {
			return store.isInMemory();
		}

		@Override
		boolean isSet(Boolean value) {
			return value != null;
		}
	}

}
