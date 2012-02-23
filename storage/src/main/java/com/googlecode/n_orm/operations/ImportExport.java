package com.googlecode.n_orm.operations;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.consoleannotations.Trigger;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.Row;

public class ImportExport {
	private static class Element implements Row, Serializable {
		private static final long serialVersionUID = -8217112442099719281L;
		
		private String key;
		private Class<? extends PersistingElement> clazz;
		private Map<String, Map<String, byte[]>> values;
		
		public Element(PersistingElement pe) {
			pe.checkIsValid();
			pe.updateFromPOJO();
			this.clazz = pe.getClass();
			this.key = pe.getIdentifier();
			Collection<ColumnFamily<?>> fams = pe.getColumnFamilies();
			values = new TreeMap<String, Map<String,byte[]>>();
			for (ColumnFamily<?> family : fams) {
				Map<String, byte[]> familyMap = new TreeMap<String, byte[]>();
				values.put(family.getName(), familyMap);
				for (String qualifier : family.getKeys()) {
					Object element = family.getElement(qualifier);
					Class<?> expected;
					if (family.getProperty() != null) {
						expected = family.getClazz();
					} else if (element instanceof PropertyManagement.Property) {
						Field propField = ((PropertyManagement.Property)element).getField();
						if (propField == null)
							continue;
						expected = propField.getType();
					} else {
						assert false;
						expected = element.getClass();
					}
					familyMap.put(qualifier, ConversionTools.convert(element, expected));
				}
			}
		}
	
		@Override
		public String getKey() {
			return key;
		}
	
		@Override
		public Map<String, Map<String, byte[]>> getValues() {
			return values;
		}
		
		public PersistingElement getElement() {
			PersistingElement ret = KeyManagement.getInstance().createElement(this.clazz, this.key);
			ret.activateFromRawData(ret.getColumnFamilyNames(), this.getValues());
			return ret;
		}
		
	}

	public static class ExportReport {
		private final PersistingElement element;
		private final long exportedElements;
		public ExportReport(PersistingElement element, long exportedElements) {
			super();
			this.element = element;
			this.exportedElements = exportedElements;
		}
		public PersistingElement getElement() {
			return element;
		}
		public long getExportedElements() {
			return exportedElements;
		}
	}

	public static final String SERIALIZATION_SEPARATOR = "n-orm";

	private ImportExport() {}

	/**
	 * Serialize a binary representation for elements in an OutputStream.
	 * Dependencies are not serialized.
	 * Elements are removed from cache to avoid memory consumption.
	 * @param elementsIterator an iterator over the elements to be serialized ; closed by the method
	 * @return lastElement the last element serialized from the collection
	 */
	public static ExportReport exportPersistingElements(CloseableIterator<? extends PersistingElement> elementsIterator, OutputStream out) throws IOException {
		ObjectOutputStream oos= new ObjectOutputStream(out);
		PersistingElement lastElement = null;
		KeyManagement km = KeyManagement.getInstance();
		long exported = 0;
		try {
			while (elementsIterator.hasNext()) {
				PersistingElement elt = elementsIterator.next();
				elt.checkIsValid();
				elt.updateFromPOJO();
				oos.writeObject(SERIALIZATION_SEPARATOR);
				oos.writeObject(new Element(elt));
				lastElement = elt;
				km.unregister(elt);
				exported++;
			}
			oos.flush();
		} finally {
			elementsIterator.close();
		}
		
		return new ExportReport(lastElement, exported);
	}

	/**
	 * Import a serialized set in a InputStream. Each element is loaded with data found from the input stream and stored.
	 * Elements are removed from cache to avoid memory consumption.
	 * @param fis the input stream to import from ; must support {@link InputStream#markSupported()}
	 * @return the number of imported elements
	 */
	public static long importPersistingElements(InputStream fis) throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		if (!fis.markSupported())
			fis = new BufferedInputStream(fis);
		
		ObjectInputStream ois = new ObjectInputStream(fis);
		KeyManagement km = KeyManagement.getInstance();
		long ret = 0;
		boolean ok = true;
		while(ok && fis.available()>0) {
			fis.mark(SERIALIZATION_SEPARATOR.getBytes().length*2);
			try {
				String sep = (String) ois.readObject();
				ok = SERIALIZATION_SEPARATOR.equals(sep);
			} catch (Exception x) {
				fis.reset();
				ok = false;
			}
			if (ok) {
				Element elt = (Element)ois.readObject();
				PersistingElement pe = elt.getElement();
				pe.delete(); //To be sure that store will get only read data
				for (ColumnFamily<?> cf : pe.getColumnFamilies()) {
					cf.setAllChanged();
				}
				pe.store();
				km.unregister(pe);
				ret++;
			}
		}
		return ret;
	}

	/**
	 * Import a serialized set from a file. Each element is loaded with data found from the file and stored.
	 * Elements are removed from cache to avoid memory consumption.
	 * @param file the file to import from
	 * @return the number of imported elements
	 */
	@Trigger
	public static long importPersistingElements(String file) throws DatabaseNotReachedException, IOException, ClassNotFoundException {
		return importPersistingElements(new BufferedInputStream(new FileInputStream(file)));
	}
}
