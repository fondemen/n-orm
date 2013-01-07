package com.googlecode.n_orm;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cache.write.WriteRetentionStore;
import com.googlecode.n_orm.storeapi.DelegatingStore;
import com.googlecode.n_orm.storeapi.SimpleStore;
import com.googlecode.n_orm.storeapi.Store;
import com.googlecode.n_orm.storeapi.SimpleStoreWrapper;
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
	public static final String STORE_REFERENCE = "as-for-package";
	public static final String STORE_WRITE_RETENTION = "with-write-retention";

	private Map<String, Object> locks = new TreeMap<String, Object>();
	private Map<String, StoreProperties> classStores = new TreeMap<String, StoreProperties>();
	private Map<String, StoreProperties> packageStores = new TreeMap<String, StoreProperties>();
	
	private transient Store PersistingElement.store = null;
	
	public Store PersistingElement.getStore() {
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
		this.store = store;
	}
	
	public Store toTypeAwareStore(SimpleStore store) {
		if (store instanceof Store) {
			return (Store)store;
		} else {
			return SimpleStoreWrapper.getWrapper(store);
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
	    	File dir = new File(clazz.getName().replace('.', '/') /*+ ".class"*/).getParentFile();
	    	//@ TODO: consider using thread group to find class loaders of all threads
	    	List<ClassLoader> classloaders = Arrays.asList(ClassLoader.getSystemClassLoader(), Thread.currentThread().getContextClassLoader(), clazz.getClassLoader());
	    	
	    	try {
	    		StoreProperties ret = this.findPropertiesInt(dir, classloaders);
	    		classStores.put(clazz.getName(), ret);
	    		return ret;
	    	} catch (IOException x) {
	    		throw new IOException("Cannot find storage properties for class " + clazz, x.getCause());
	    	}
    	}
    }
    
    private StoreProperties findPropertiesInt(File dir, List<ClassLoader> classLoaders) throws IOException {
		if (dir == null)
			throw new IOException();
    		
		final String pack = dir.getPath().replace('/', '.').replace('\\', '.');
		synchronized (getLock(pack)) {
			StoreProperties ret = packageStores.get(pack);
    		if (ret != null) {
    			//assert ret.pack.equals(pack) && ret.properties != null; //Actually, not in case of a STORE_REFERENCE
    			return ret;
    		}
    		
    		File f = new File(dir, PROPERTY_FILE);
    		String path = f.getPath();
    		//Evil windows states that \ is a separator while / is expected by getResourceAsStream
    		if (File.separatorChar == '\\')
    			path = path.replace('\\', '/');
    		InputStream in = null;
    		for (ClassLoader loader : classLoaders) {
				in = loader.getResourceAsStream(path);
				if (in != null) break;
			}
    			
    		if (in != null)
	    		try {
	    	    	Properties res = new Properties();
	    			res.load(in);
	    			
	    			//Check whether this is a reference to another file
	    			String ref = res.getProperty(STORE_REFERENCE);
	    			if (ref != null) {
	    				//Let's continue searching from the given path
	    				dir = new File(ref.replace('.', '/').replace('\\', '/'), PROPERTY_FILE);
	    			} else { //We found it and it's not an indirection
		    			ret = new StoreProperties(res, pack);
		    			packageStores.put(ret.pack, ret);
		    			return ret;
	    			}
	    		} catch (IOException x) {
	    		}
		}
		
		//Not found ; let's try in the package above...
		try {
			StoreProperties ret = this.findPropertiesInt(dir.getParentFile(), classLoaders);
			packageStores.put(pack, ret);
			return ret;
		} catch (IOException x) {
			throw new IOException("Cannot find storage properties for package " + pack, x.getCause());
		}
    	
    }
    
    //For test purpose.
    public void setPropertiesFor(Class<? extends PersistingElement> clazz, Properties properties) {
    	synchronized (this.getLock(clazz)) {
    		this.classStores.put(clazz.getName(), new StoreProperties(properties, clazz.getPackage().getName()));
    	}
    }
    
    //For test purpose.
    public void setPropertiesFor(Class<? extends PersistingElement> clazz, Store store) {
    	synchronized (this.getLock(clazz)) {
    		Properties props = new Properties();
    		props.setProperty(STORE_DRIVERCLASS_PROPERTY, store.getClass().getName());
    		StoreProperties sprop = new StoreProperties(new Properties(), clazz.getPackage().getName());
    		sprop.store = store;
    		this.classStores.put(clazz.getName(), sprop);
    	}
    }
	
	public Store getStoreFor(Class<? extends PersistingElement> clazz) throws DatabaseNotReachedException {
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
					ret = checkForRetention(ret, clazz);
					classStores.put(clazz.getName(), ret);
					return ret.store;
				}
			}
			
			try {
				if (ret == null) {
					ret = findPropertiesInt(clazz);
					if (ret.store != null) {
						ret = checkForRetention(ret, clazz);
						classStores.put(clazz.getName(), ret);
						return ret.store;
					}
				}
				
				assert ret.store == null && ret.properties != null;
				Properties properties = ret.properties;
	
				Class<?> storeClass = Class.forName(properties.getProperty(STORE_DRIVERCLASS_PROPERTY));
				Object store;
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
					store = accessor.invoke(null, args.toArray());
				} else if (properties.containsKey(STORE_DRIVERCLASS_SINGLETON_PROPERTY))
					store = PropertyManagement.getInstance().readValue(null, storeClass.getField(properties.getProperty(STORE_DRIVERCLASS_SINGLETON_PROPERTY)));
				else
					store = storeClass.newInstance();
				
				assert store != null;
				
				for (PropertyDescriptor property : PropertyUtils.getPropertyDescriptors(store)) {
					if (PropertyUtils.isWriteable(store, property.getName()) && properties.containsKey(property.getName())) {
						PropertyUtils.setProperty(store, property.getName(), ConvertUtils.convert(properties.getProperty(property.getName()), property.getPropertyType()));
					}
				}
				
				if (store instanceof Store)
					ret.store = (Store)store;
				else if (store instanceof SimpleStore)
					ret.store = SimpleStoreWrapper.getWrapper((SimpleStore)store);
				else
					throw new IllegalArgumentException("Error while getting store for class " + clazz.getName() + ": found store " + store.toString() + " of class " + store.getClass().getName() + " which is not compatible with " + Store.class.getName() + " or " + SimpleStore.class.getName());

				if (ret.properties.containsKey(STORE_WRITE_RETENTION)) {
					String wrStr = ret.properties.getProperty(STORE_WRITE_RETENTION);
					boolean disabled = wrStr.endsWith("-disabled");
					if (disabled) {
						wrStr = wrStr.substring(0, wrStr.length() - "-disabled".length());
					}
					WriteRetentionStore wrs = WriteRetentionStore.getWriteRetentionStore(Long.parseLong(wrStr), ret.store);
					if(disabled)
						wrs.setEnabledByDefault(false);
					ret.store = wrs;
				}

				ret = checkForRetention(ret, clazz);
				
				ret.store.start();
				classStores.put(clazz.getName(), ret);
				return ret.store;
			} catch (Exception x) {
				throw new DatabaseNotReachedException(x);
			}
		}
	}

	/**
	 * Get store for given class bypassing any {@link com.googlecode.n_orm.storeapi.DelegatingStore}
	 */
	public Store getActualStoreFor(Class<? extends PersistingElement> clazz) throws DatabaseNotReachedException {
		Store ret = this.getStoreFor(clazz);
		return ret instanceof DelegatingStore ? ((DelegatingStore)ret).getDeepActualStore() : ret;
	}
	
	private StoreProperties checkForRetention(StoreProperties sp,
			Class<? extends PersistingElement> clazz) {
		assert sp.store != null;
		Persisting pa = clazz.getAnnotation(Persisting.class);
		if (pa.writeRetentionMs() > 0) {
			StoreProperties ret = new StoreProperties(sp.properties, sp.pack);
			ret.store = WriteRetentionStore.getWriteRetentionStore(pa.writeRetentionMs(), sp.store);
			return ret;
		} else
			return sp;
	}
}
