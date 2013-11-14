package com.googlecode.n_orm;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

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
	
	private static enum FileFormat {
		JSON, PROPERTIES
	}
	
	public static class StoreProperties implements Comparable<StoreProperties> {
		public final Map<String, Object> properties;
		public final String pack;
		public Store store;
		
		public StoreProperties(Map<String, Object> properties, String pack) {
			this.properties = properties;
			this.pack = pack;
		}
		
		@Override
		public int compareTo(StoreProperties rhs) {
			return pack.compareTo(rhs.pack);
		}
	}

	public static final String PROPERTY_FILE = "store.properties";
	public static final String JSON_FILE = "store.json";
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
	public Map<String, Object> findProperties(Class<?> clazz) throws IOException {
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
    
    private InputStream getInputStreamForProperties(File dir, String fileName, List<ClassLoader> classLoaders) {
		File f = new File(dir, fileName);
		String path = f.getPath();
		//Evil windows states that \ is a separator while / is expected by getResourceAsStream
		if (File.separatorChar == '\\') {
			path = path.replace('\\', '/');
		}
		InputStream in = null;
		for (ClassLoader loader : classLoaders) {
			in = loader.getResourceAsStream(path);
			if (in != null) {
				return in;
			}
		}
		return null;
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
    		
    		
    		// Trying to get JSON
    		InputStream in = this.getInputStreamForProperties(dir, JSON_FILE, classLoaders);
    		FileFormat format = FileFormat.JSON;
    		
    		// Trying to get properties
    		if (in == null) {
    			in = this.getInputStreamForProperties(dir, PROPERTY_FILE, classLoaders);
    			format = FileFormat.PROPERTIES;
    		}
    			
    		if (in != null)
	    		try {
	    			Map<String, Object> res;
	    	    	
	    	    	switch (format) {
	    	    	case PROPERTIES:
	    	    		Properties props = new Properties();
	    	    		props.load(in);
	    	    		res = new HashMap<String, Object>();
	    	    		for (Entry<Object, Object> prop : props.entrySet()) {
							res.put((String) prop.getKey(), prop.getValue());
						}
	    	    		break;
	    	    	case JSON:
	    	    		try {
							res = (JSONObject)new JSONParser().parse(new InputStreamReader(in));
						} catch (Exception e) {
							throw new DatabaseNotReachedException("Problem while loading store properties " + dir + '/' + JSON_FILE, e);
						}
	    	    		break;
	    	    	default:
	    	    		throw new Error("Cannot parse " + format);
	    	    	}
	    			
	    			//Check whether this is a reference to another file
	    			Object ref = res.get(STORE_REFERENCE);
	    			if ((ref != null) && (ref instanceof String)) {
	    				//Let's continue searching from the given path
	    				dir = new File(((String)ref).replace('.', '/').replace('\\', '/'), PROPERTY_FILE);
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
    public void setPropertiesFor(Class<? extends PersistingElement> clazz, Map<String, Object> properties) {
    	synchronized (this.getLock(clazz)) {
    		this.classStores.put(clazz.getName(), new StoreProperties(properties, clazz.getPackage().getName()));
    	}
    }
    
    //For test purpose.
    public void setPropertiesFor(Class<? extends PersistingElement> clazz, Store store) {
    	synchronized (this.getLock(clazz)) {
    		Properties props = new Properties();
    		props.setProperty(STORE_DRIVERCLASS_PROPERTY, store.getClass().getName());
    		StoreProperties sprop = new StoreProperties(new TreeMap<String, Object>(), clazz.getPackage().getName());
    		sprop.store = store;
    		this.classStores.put(clazz.getName(), sprop);
    	}
    }
    
    /**
     * Get an instance of Store according to properties.
     * @param builderName The name of a static method to get the store ; constructor if null
     * @param storeClass The class of the store to be built
     * @param properties The property descriptor
     */
    private Object buildStore(String builderName, Class<?> storeClass, Map<String, Object> properties) {
		//Detecting parameters number
		int pmnr = 0;
		while (properties.get(Integer.toString(pmnr+1)) != null)
			pmnr++;
		
		// Parameters values
		List<Object> args = new ArrayList<Object>(pmnr);
		
		if (builderName == null) {
			//Finding constructor to invoke
			for (Constructor<?> c : storeClass.getConstructors()) {
				if (c.getParameterTypes().length == pmnr) {
					try {
						populateArguments(properties, c.getParameterTypes(), args);
						return c.newInstance(args.toArray());
					} catch (Exception x) {}
				}
			}
			throw new IllegalArgumentException("Could not find constructor with compatible " + pmnr + " arguments in " + storeClass);
		} else {
			//Finding static method to invoke
			for (Method m : storeClass.getMethods()) {
				if (m.getName().equals(builderName) && (m.getModifiers() & Modifier.STATIC) != 0 && m.getParameterTypes().length == pmnr) {
					try {
						populateArguments(properties, m.getParameterTypes(), args);
						return m.invoke(null, args.toArray());
					} catch (Exception x) {}
				}
			}
			throw new IllegalArgumentException("Could not find static accessor method " + builderName + " with compatible " + pmnr + " arguments in " + storeClass);
		}
    }
    
    private void populateArguments(Map<String, Object> properties, Class<?>[] types, List<Object> args) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, NoSuchFieldException {
		int i = 1;
		for (Class<?> c : types) {
			Object val = properties.get(Integer.toString(i));
			if (val == null) {
				throw new IllegalArgumentException("Missing required value " + Integer.toString(i));
			}
			val = convert(val, c);
			args.add(val);
			i++;
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
	
				// Building store
				Object store = toObject(ret.properties);
				if (!(store instanceof Store)) {
					throw new IllegalArgumentException("Error while loading store for " + clazz + " properties describe " + store + " while expecting an instance of " + Store.class.getName());
				}
				ret.store = (Store)store;
				
				// Checking for write retention in (flat) properties
				if (ret.properties.containsKey(STORE_WRITE_RETENTION)) {
					String wrStr = (String)ret.properties.get(STORE_WRITE_RETENTION);
					boolean disabled = wrStr.endsWith("-disabled");
					if (disabled) {
						wrStr = wrStr.substring(0, wrStr.length() - "-disabled".length());
					}
					WriteRetentionStore wrs = WriteRetentionStore.getWriteRetentionStore(Long.parseLong(wrStr), ret.store);
					if(disabled)
						wrs.setEnabledByDefault(false);
					ret.store = wrs;
				}

				// Checking for write retention in annotation
				ret = checkForRetention(ret, clazz);
				
				ret.store.start();
				classStores.put(clazz.getName(), ret);
				return ret.store;
			} catch (Exception x) {
				throw new DatabaseNotReachedException(x);
			}
		}
	}
	
	private Object toObject(Map<String, Object> properties)
			throws ClassNotFoundException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			NoSuchFieldException {
		Class<?> storeClass = Class.forName((String)properties.get(STORE_DRIVERCLASS_PROPERTY));
		Object ret;
		if (properties.containsKey(STORE_DRIVERCLASS_STATIC_ACCESSOR)) {
			String accessorName = (String)properties.get(STORE_DRIVERCLASS_STATIC_ACCESSOR);
			ret = buildStore(accessorName, storeClass, properties);
		} else if (properties.containsKey(STORE_DRIVERCLASS_SINGLETON_PROPERTY)) {
			ret = PropertyManagement.getInstance().readValue(null, storeClass.getField((String)properties.get(STORE_DRIVERCLASS_SINGLETON_PROPERTY)));
		} else { 
			ret = buildStore(null, storeClass, properties);
		}
		
		assert ret != null;
		
		// Setting properties
		for (PropertyDescriptor property : PropertyUtils.getPropertyDescriptors(ret)) {
			if (PropertyUtils.isWriteable(ret, property.getName()) && properties.containsKey(property.getName())) {
				PropertyUtils.setProperty(ret, property.getName(), convert(properties.get(property.getName()), property.getPropertyType()));
			}
		}
		
		// Wrapping if necessary
		if (ret instanceof SimpleStore)
			ret = SimpleStoreWrapper.getWrapper((SimpleStore)ret);
		
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	private Object convert(Object val, Class<?> c) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, NoSuchFieldException {
		if (val instanceof Map) {
			Object ret = toObject((Map<String, Object>) val);
			if (! c.isInstance(ret)) {
				throw new ClassCastException(ret + " as described by properties " + val + " is not instance of " + c);
			} else {
				return ret;
			}
		} else {
			val = val.toString();
			if (c.isEnum()) {
				return Enum.valueOf(c.asSubclass(Enum.class), (String)val);
			}
			return ConvertUtils.convert((String)val, c);
		}
		//@ TODO support arrays
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
