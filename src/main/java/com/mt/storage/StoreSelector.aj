package com.mt.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.beanutils.ConvertUtils;

public aspect StoreSelector {
	public static final String PROPERTY_FILE = "store.properties";
	public static final String STORE_DRIVERCLASS_PROPERTY = "class";
	public static final String STORE_DRIVERCLASS_SINGLETON_PROPERTY = "singleton";
	public static final String STORE_DRIVERCLASS_STATIC_ACCESSOR = "static-accessor";

	
	public Map<Class<? extends PersistingElement>, Properties> properties = new HashMap<Class<? extends PersistingElement>, Properties>();
	
	private transient Store PersistingElement.store = null;
	
	public Store PersistingElement.getStore() {
		if (this.store == null)
			try {
				this.store = StoreSelector.aspectOf().getStoreFor(this.getClass());
			} catch (DatabaseNotReachedException x) {
				throw new IllegalStateException(x);
			}
		return this.store;
	}
	
	public void PersistingElement.setStore(Store store) {
		if (this.store != null)
			throw new IllegalStateException("A store is already registered for object " + (this.getIdentifier() == null ? "" : this.getIdentifier()) + " of class " + this.getClass().getName());
		this.store = store;
	}
    
    public File findProperties(Class<?> clazz) throws IOException {
    	File f = new File(clazz.getClassLoader().getResource(clazz.getName().replace('.', '/') + ".class").getFile()), dir = f;
    	do {
    		dir = dir.getParentFile();
    		if (dir == null || !dir.isDirectory())
    			throw new IOException("No property file found for class " + clazz);
    		f = new File(dir, PROPERTY_FILE);
    	} while (!f.exists() || !f.isFile());
    	return f;
    }
    
    //For test purpose.
    public void setPropertiesFor(Class<? extends PersistingElement> clazz, Properties properties) {
    	this.properties.put(clazz, properties);
    }
	
	public Store getStoreFor(Class<? extends PersistingElement> clazz) throws DatabaseNotReachedException {
		Properties properties = this.properties.get(clazz);
		try {
			if (properties == null) {
				properties = new Properties();
				properties.load(new FileInputStream(findProperties(clazz)));
				this.setPropertiesFor(clazz, properties);
			}
			@SuppressWarnings("unchecked")
			Class<Store> storeClass = (Class<Store>) Class.forName(properties.getProperty(STORE_DRIVERCLASS_PROPERTY));
			Store ret;
			if (properties.containsKey(STORE_DRIVERCLASS_STATIC_ACCESSOR)) {
				String accessorName = properties.getProperty(STORE_DRIVERCLASS_STATIC_ACCESSOR);
				Method accessor = null;
				//Detecting parameters number
				int pmnr = 0;
				while (properties.getProperty(Integer.toString(pmnr+1)) != null)
					pmnr++;
				//Finding static method to invoke
				for (Method m : storeClass.getMethods()) {
					if (m.getName().equals(accessorName) && (m.getModifiers() & Modifier.STATIC) != 0 && m.getParameterTypes().length == pmnr) {
						accessor = m;
						break;
					}
				}
				if (accessor == null)
					throw new IllegalArgumentException("Could not find static accessor method " + accessorName + " in class " + storeClass);
				List<Object> args = new ArrayList<Object>(pmnr);
				int i = 1;
				for (Class<?> c : accessor.getParameterTypes()) {
					String valAsString = properties.getProperty(Integer.toString(i));
					if (valAsString == null)
						throw new IllegalArgumentException("Missing required value " + Integer.toString(i));
					Object val = ConvertUtils.convert(valAsString, c);
					args.add(val);
					i++;
				}
				ret = (Store) accessor.invoke(null, args.toArray());
			} else if (properties.containsKey(STORE_DRIVERCLASS_SINGLETON_PROPERTY))
				ret = (Store) PropertyManagement.aspectOf().readValue(null, storeClass.getField(properties.getProperty(STORE_DRIVERCLASS_SINGLETON_PROPERTY)));
			else
				ret = storeClass.newInstance();
			
			for (Field property : PropertyManagement.aspectOf().getProperties(storeClass)) {
				if (properties.containsKey(property.getName())) {
					PropertyManagement.aspectOf().setValue(ret, property, ConvertUtils.convert(properties.getProperty(property.getName()), property.getType()));
				}
			}
			
			ret.start();
			
			return ret;
		} catch (Exception x) {
			throw new DatabaseNotReachedException(x);
		}
	}
}
