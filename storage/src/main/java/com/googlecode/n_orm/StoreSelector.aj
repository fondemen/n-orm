package com.googlecode.n_orm;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.TypeAwareStore;
import com.googlecode.n_orm.storeapi.TypeAwareStoreWrapper;
import com.googlecode.n_orm.StoreSelector;

public aspect StoreSelector {
	private static StoreSelector INSTANCE;
	
	public static StoreSelector getInstance() {
		if (INSTANCE == null)
			INSTANCE = aspectOf();
		return INSTANCE;
	}
	
	public static class StoreProperties implements Comparable<StoreProperties> {
		public final Properties properties;
		public final String pack;
		public Store store;
		
		public StoreProperties(Properties properties, String pack) {
			this.properties = properties;
			this.pack = pack;
		}
		
		@Override
		public int compareTo(StoreProperties rhs) {
			return pack.compareTo(rhs.pack);
		}
	}
	
	public static final String PROPERTY_FILE = "store.properties";
	public static final String STORE_DRIVERCLASS_PROPERTY = "class";
	public static final String STORE_DRIVERCLASS_SINGLETON_PROPERTY = "singleton";
	public static final String STORE_DRIVERCLASS_STATIC_ACCESSOR = "static-accessor";

	private Map<String, Object> locks = new TreeMap<String, Object>();
	private Map<String, StoreProperties> classStores = new TreeMap<String, StoreProperties>();
	private Map<String, StoreProperties> packageStores = new TreeMap<String, StoreProperties>();
	
	private transient TypeAwareStore PersistingElement.store = null;
	
	public TypeAwareStore PersistingElement.getStore() {
		if (this.store == null)
			synchronized(this) {
				if (this.store == null)
					try {
						this.setStore(StoreSelector.getInstance().getStoreFor(this.getClass()));
					} catch (DatabaseNotReachedException x) {
						throw new IllegalStateException(x);
					}
			}
		return this.store;
	}
	
	public void PersistingElement.setStore(Store store) {
		if (this.store != null)
			throw new IllegalStateException("A store is already registered for object " + (this.getIdentifier() == null ? "" : this.getIdentifier()) + " of class " + this.getClass().getName());
		this.store = StoreSelector.getInstance().toTypeAwareStore(store);
	}
	
	public TypeAwareStore toTypeAwareStore(Store store) {
		if (store instanceof TypeAwareStore) {
			return (TypeAwareStore)store;
		} else {
			return TypeAwareStoreWrapper.getWrapper(store);
		}
	}
	
	private Object getLock(Class<?> clazz) {
		return this.getLock(clazz.getName());
	}
	
	private Object getLock(Package pack) {
		return this.getLock(pack.getName());
	}
	
	private synchronized Object getLock(String elementId) {
		Object ret = this.locks.get(elementId);
		if (ret == null) {
			ret = new Object();
			this.locks.put(elementId, ret);
		}
		return ret;
	}
	
	//For test purpose
	public Properties findProperties(Class<?> clazz) throws IOException {
		return this.findPropertiesInt(clazz).properties;
	}
    
    private StoreProperties findPropertiesInt(Class<?> clazz) throws IOException {
    	synchronized (this.getLock(clazz)) {
	    	File f = new File(clazz.getName().replace('.', '/') + ".class"), dir = f;
	    	Properties res = new Properties();
	    	//@ TODO: consider using thread group to find class loaders of all threads
	    	ClassLoader [] loaders = {ClassLoader.getSystemClassLoader(), Thread.currentThread().getContextClassLoader(), clazz.getClassLoader()};
	    	do {
	    		StoreProperties ret;
	    		
	    		dir = dir.getParentFile();
	    		if (dir == null)
	    			throw new IOException("Cannot find storage properties for class " + clazz);
	    		
	    		String pack = dir.getPath().replace('/', '.').replace('\\', '.');
	    		synchronized (getLock(pack)) {
		    		ret = packageStores.get(pack);
		    		if (ret != null) {
		    			assert ret.pack.equals(pack) && ret.properties != null;
		    			return ret;
		    		}
		    		
		    		f = new File(dir, PROPERTY_FILE);
		    		String path = f.getPath();
		    		//Evil windows states that \ is a separator while / is expected by getResourceAsStream
		    		if (File.separatorChar == '\\')
		    			path = path.replace('\\', '/');
		    		InputStream in = null;
		    		for (ClassLoader loader : loaders) {
						in = loader.getResourceAsStream(path);
						if (in != null) break;
					}
		    			
		    		if (in != null)
			    		try {
			    			res.load(in);
			    			ret = new StoreProperties(res, pack);
			    			packageStores.put(ret.pack, ret);
			    			return ret;
			    		} catch (IOException x) {
			    		}
	    		}
	    	} while (true);
    	}
    }
    
    //For test purpose.
    public void setPropertiesFor(Class<? extends PersistingElement> clazz, Properties properties) {
    	synchronized (this.getLock(clazz)) {
    		this.classStores.put(clazz.getName(), new StoreProperties(properties, clazz.getPackage().getName()));
    	}
    }
	
	public synchronized Store getStoreFor(Class<? extends PersistingElement> clazz) throws DatabaseNotReachedException {
		synchronized (this.getLock(clazz)) {
			StoreProperties ret;
			
			ret = classStores.get(clazz.getName());
			if (ret != null) {
				if (ret.store != null)
					return ret.store;
			} else {
				synchronized (getLock(clazz.getPackage())) {
					ret = packageStores.get(clazz.getPackage().getName());
				}
				if (ret != null && ret.store != null) {
					classStores.put(clazz.getName(), ret);
					return ret.store;
				}
			}
			
			try {
				if (ret == null) {
					ret = findPropertiesInt(clazz);
					if (ret.store != null) {
						classStores.put(clazz.getName(), ret);
						return ret.store;
					}
				}
				
				assert ret.store == null && ret.properties != null;
				Properties properties = ret.properties;
	
				@SuppressWarnings("unchecked")
				Class<Store> storeClass = (Class<Store>) Class.forName(properties.getProperty(STORE_DRIVERCLASS_PROPERTY));
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
					ret.store = (Store) accessor.invoke(null, args.toArray());
				} else if (properties.containsKey(STORE_DRIVERCLASS_SINGLETON_PROPERTY))
					ret.store = (Store) PropertyManagement.getInstance().readValue(null, storeClass.getField(properties.getProperty(STORE_DRIVERCLASS_SINGLETON_PROPERTY)));
				else
					ret.store = storeClass.newInstance();
				
				assert ret.store != null;
				
				for (PropertyDescriptor property : PropertyUtils.getPropertyDescriptors(ret.store)) {
					if (PropertyUtils.isWriteable(ret.store, property.getName()) && properties.containsKey(property.getName())) {
						PropertyUtils.setProperty(ret.store, property.getName(), ConvertUtils.convert(properties.getProperty(property.getName()), property.getPropertyType()));
					}
				}
	
				ret.store.start();
				classStores.put(clazz.getName(), ret);
				return ret.store;
			} catch (Exception x) {
				throw new DatabaseNotReachedException(x);
			}
		}
	}
}
