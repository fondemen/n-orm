package com.googlecode.n_orm.operations;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.googlecode.n_orm.CloseableIterator;
import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.cf.ColumnFamily;
import com.googlecode.n_orm.consoleannotations.Trigger;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.Row;

public class ImportExport {
	private static class Element implements Row, Serializable {
		private static final long serialVersionUID = -8217112442099719281L;
		
		private String key;
		private Class<? extends PersistingElement> clazz;
		private ColumnFamilyData values;
		
		public Element(PersistingElement pe) {
			pe.checkIsValid();
			pe.updateFromPOJO();
			this.clazz = pe.getClass();
			this.key = pe.getIdentifier();
			Collection<ColumnFamily<?>> fams = pe.getColumnFamilies();
			values = new DefaultColumnFamilyData();
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
		public ColumnFamilyData getValues() {
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

	/**
	 * An exception rose during a deserialization process.
	 */
	public static class ProcessReadException extends Exception {
		private final PersistingElement element;
		private final InputStream input;
		private ProcessReadException(PersistingElement element,
				Throwable exception, InputStream input) {
			super("Problem while reading element " + element, exception);
			this.element = element;
			this.input = input;
		}
		protected PersistingElement getElement() {
			return element;
		}
		protected InputStream getInput() {
			return input;
		}
	}
	
	/**
	 * The list of exception raised during a read process.
	 */
	public static class ReadException extends Exception {
		private final Collection<ProcessReadException> exceptions;
		
		public ReadException(Collection<ProcessReadException> exceptions) {
			super(exceptions.size() + " problems while reading serialized elements", exceptions.iterator().next());
			this.exceptions = Collections.unmodifiableCollection(exceptions);
		}

		protected Collection<ProcessReadException> getExceptions() {
			return exceptions;
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
	public static ExportReport exportPersistingElements(CloseableIterator<? extends PersistingElement> elementsIterator, ObjectOutputStream out) throws IOException {
		PersistingElement lastElement = null;
		KeyManagement km = KeyManagement.getInstance();
		long exported = 0;
		try {
			while (elementsIterator.hasNext()) {
				PersistingElement elt = elementsIterator.next();
				elt.checkIsValid();
				elt.updateFromPOJO();
				out.writeObject(SERIALIZATION_SEPARATOR);
				out.writeObject(new Element(elt));
				lastElement = elt;
				km.unregister(elt);
				exported++;
				if (exported % 100 == 0) {
					out.reset();
				}
			}
			out.flush();
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
	public static long importPersistingElements(InputStream fis) throws DatabaseNotReachedException, IOException, ClassNotFoundException, ReadException {
		return readSerializedPersistingElements(fis, new Process<PersistingElement>() {

			@Override
			public void process(PersistingElement element) throws Throwable {
				element.store();
			}
		});
	}
	
	/**
	 * Processes sequentially each element of a serialized stream without importing them.
	 * Each element is removed from cache as soon as it is processed so as to avoid memory consumption.
	 * @param fis the input stream of elements
	 * @param process the process to be applied to each element
	 * @return the number of processed elements, including those that rose an error
	 * @throws ReadException errors while processing elements
	 */
	public static <T extends PersistingElement> long readSerializedPersistingElements(InputStream fis, Process<T> process) throws DatabaseNotReachedException, IOException, ClassNotFoundException, ReadException {
		if (!fis.markSupported())
			fis = new BufferedInputStream(fis);
		
		ObjectInputStream ois = new ObjectInputStream(fis);
		KeyManagement km = KeyManagement.getInstance();
		long ret = 0;
		boolean ok = true;

		List<ProcessReadException> problems = new LinkedList<ProcessReadException>();
		while(ok && fis.available()>0) {
			fis.mark(SERIALIZATION_SEPARATOR.getBytes().length*2);
			try {
				String sep = (String) ois.readObject();
				ok = SERIALIZATION_SEPARATOR.equals(sep);
			} catch (Exception x) {
				x.printStackTrace();
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
				try {
					process.process((T)pe);
				} catch (Throwable e) {
					problems.add(new ProcessReadException(pe, e, fis));
				}
				km.unregister(pe);
				ret++;
			}
		}
		if (!problems.isEmpty()) {
			throw new ReadException(problems);
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
	public static long importPersistingElements(String file) throws DatabaseNotReachedException, IOException, ClassNotFoundException, ReadException {
		BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
		try {
			return importPersistingElements(fis);
		} finally {
			fis.close();
		}
	}

	/**
	 * Processes sequentially each element of a serialized file without importing them.
	 * Each element is removed from cache as soon as it is processed so as to avoid memory consumption.
	 * @param file the file to import from
	 * @param process the process to be applied to each element
	 * @return the number of processed elements, including those that rose an error
	 * @throws ReadException errors while processing elements
	 */
	public static long readSerializedPersistingElements(String file, Process<? extends PersistingElement> process) throws DatabaseNotReachedException, IOException, ClassNotFoundException, ReadException {
		BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
		try {
			return readSerializedPersistingElements(fis, process);
		} finally {
			fis.close();
		}
	}
}
