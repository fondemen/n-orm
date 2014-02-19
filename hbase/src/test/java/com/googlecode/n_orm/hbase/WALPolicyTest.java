/*package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.KeyManagement;
import com.googlecode.n_orm.Persisting;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.hbase.actions.Action;
import com.googlecode.n_orm.hbase.actions.BatchAction;
import com.googlecode.n_orm.storeapi.DefaultColumnFamilyData;
import com.googlecode.n_orm.storeapi.MetaInformation;
import com.googlecode.n_orm.storeapi.Row.ColumnFamilyData;
import com.googlecode.n_orm.hbase.HBaseSchema.WALWritePolicy;

public class WALPolicyTest {

	public static Store store;

	@BeforeClass
	public static void prepareStore() {
		HBaseLauncher.prepareHBase();
		store = spy(HBaseLauncher.hbaseStore);
	}

	public List<Action<?>> performedActions = new LinkedList<Action<?>>();

	@SuppressWarnings("unchecked")
	@Before
	public void resetMocks() {
		reset(store);
		doReturn(null).when(store).tryPerform(any(Action.class),
				any(HTable.class), any(Class.class), anyString(),
				any(Map.class));
	}

	@SuppressWarnings("unchecked")
	public Put getLastPut() {
		@SuppressWarnings("rawtypes")
		ArgumentCaptor<Action> capt = ArgumentCaptor.forClass(Action.class);
		verify(store).tryPerform(capt.capture(), any(HTable.class),
				any(Class.class), anyString(), any(Map.class));
		BatchAction action = (BatchAction) capt.getValue();
		List<Row> batch = action.getBatch();
		for (Row row : batch) {
			if (row instanceof Put) {
				return (Put) row;
			}
		}
		return null;
	}

	@Persisting
	public static class DefaultElement extends DummyPersistingElement {
		private static final long serialVersionUID = 5066914271185995634L;
		@Key
		public String key;
	}

	@Test
	public final void defaultElementNoCF() {
		DefaultElement elt = new DefaultElement();
		elt.key = "AZERTYUIO";
		store.storeChanges(new MetaInformation().forElement(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				null, null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertTrue(put.getWriteToWAL());
	}

	@Persisting
	@HBaseSchema(writeToWAL = WALWritePolicy.SKIP)
	public static class PropsNoWALElement extends DummyPersistingElement {
		private static final long serialVersionUID = 3432375616581521425L;
		@Key
		public String key;
		public String prop;
		public Set<String> cf1UnsetWalled = new TreeSet<String>();
		@HBaseSchema(writeToWAL = WALWritePolicy.UNSET)
		public Set<String> cf1ExplUnsetWalled = new TreeSet<String>();
		@HBaseSchema(writeToWAL = WALWritePolicy.SKIP)
		public Map<String, String> cf2Unwalled = new TreeMap<String, String>();
		@HBaseSchema(writeToWAL = WALWritePolicy.USE)
		public Map<String, String> cf3Walled = new TreeMap<String, String>();
	}

	public MetaInformation createMeta(PropsNoWALElement elt) {
		MetaInformation ret = new MetaInformation().forElement(elt);

		Map<String, Field> fams = new TreeMap<String, Field>();
		try {
			if (!elt.cf1UnsetWalled.isEmpty()) {
				fams.put("cf1UnsetWalled",
						PropsNoWALElement.class.getField("cf1UnsetWalled"));
			}
			if (!elt.cf1ExplUnsetWalled.isEmpty()) {
				fams.put("cf1ExplUnsetWalled",
						PropsNoWALElement.class.getField("cf1ExplUnsetWalled"));
			}
			if (!elt.cf2Unwalled.isEmpty()) {
				fams.put("cf2Unwalled",
						PropsNoWALElement.class.getField("cf2Unwalled"));
			}
			if (!elt.cf3Walled.isEmpty()) {
				fams.put("cf3Walled",
						PropsNoWALElement.class.getField("cf3Walled"));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (!fams.isEmpty()) {
			ret.withColumnFamilies(fams);
		}
		return ret;
	}

	public ColumnFamilyData createChanges(PropsNoWALElement elt) {
		ColumnFamilyData ret = new DefaultColumnFamilyData();
		if (elt.prop != null) {
			Map<String, byte[]> ch = new TreeMap<String, byte[]>();
			ch.put("prop", ConversionTools.convert(elt.prop));
			ret.put(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME, ch);
		}
		if (!elt.cf1UnsetWalled.isEmpty()) {
			Map<String, byte[]> ch = new TreeMap<String, byte[]>();
			for (String inElt : elt.cf1UnsetWalled) {
				ch.put(inElt, null);
			}
			ret.put("cf1UnsetWalled", ch);
		}
		if (!elt.cf1ExplUnsetWalled.isEmpty()) {
			Map<String, byte[]> ch = new TreeMap<String, byte[]>();
			for (String inElt : elt.cf1ExplUnsetWalled) {
				ch.put(inElt, null);
			}
			ret.put("cf1ExplUnsetWalled", ch);
		}
		if (!elt.cf2Unwalled.isEmpty()) {
			Map<String, byte[]> ch = new TreeMap<String, byte[]>();
			for (Entry<String, String> inElt : elt.cf2Unwalled.entrySet()) {
				ch.put(inElt.getKey(),
						ConversionTools.convert(inElt.getValue()));
			}
			ret.put("cf2Unwalled", ch);
		}
		if (!elt.cf3Walled.isEmpty()) {
			Map<String, byte[]> ch = new TreeMap<String, byte[]>();
			for (Entry<String, String> inElt : elt.cf3Walled.entrySet()) {
				ch.put(inElt.getKey(),
						ConversionTools.convert(inElt.getValue()));
			}
			ret.put("cf3Walled", ch);
		}
		return ret;
	}

	@Test
	public final void propsNoWalElementNothing() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				null, null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertFalse(put.getWriteToWAL());
	}

	@Test
	public final void propsNoWalElementNoCF() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		elt.prop = "jdkhsjdkfhs";
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				this.createChanges(elt), null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertFalse(put.getWriteToWAL());
	}

	@Test
	public final void propsNoWalElementUnset() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		elt.prop = "jdkhsjdkfhs";
		elt.cf1UnsetWalled.add("sjklqjdklqdsfhsldfjhs");
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				this.createChanges(elt), null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertFalse(put.getWriteToWAL());
	}

	@Test
	public final void propsNoWalElementExplUnset() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		elt.prop = "jdkhsjdkfhs";
		elt.cf1ExplUnsetWalled.add("sjklqjdklqdsfhsldfjhs");
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				this.createChanges(elt), null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertFalse(put.getWriteToWAL());
	}

	@Test
	public final void propsNoWalElementUnwalledCF() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		elt.prop = "jdkhsjdkfhs";
		elt.cf2Unwalled.put("sjklqjdklq", "dsfhsldfjhs");
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				this.createChanges(elt), null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertFalse(put.getWriteToWAL());
	}

	@Test
	public final void propsNoWalElementWalledCFOnly() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		elt.prop = "jdkhsjdkfhs";
		elt.cf3Walled.put("sjklsqsdqsqqj", "dklqdqsqssfhsldfjhs");
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				this.createChanges(elt), null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertTrue(put.getWriteToWAL());
	}

	@Test
	public final void propsNoWalElementWalledCF() {
		PropsNoWALElement elt = new PropsNoWALElement();
		elt.key = "AZERTYUIO";
		elt.prop = "jdkhsjdkfhs";
		elt.cf1UnsetWalled.add("sjklqjdklqdsfhsldfjhs");
		elt.cf2Unwalled.put("sjklsqsdqqj", "dklqdsfhsldfjhs");
		elt.cf3Walled.put("sjklsqsdqsqqj", "dklqdqsqssfhsldfjhs");
		store.storeChanges(this.createMeta(elt),
				"WALPolicyTest", "AZERTYUIO" + KeyManagement.KEY_END_SEPARATOR,
				this.createChanges(elt), null, null);
		Put put = getLastPut();
		assertNotNull(put);
		assertTrue(put.getWriteToWAL());
	}

}*/
