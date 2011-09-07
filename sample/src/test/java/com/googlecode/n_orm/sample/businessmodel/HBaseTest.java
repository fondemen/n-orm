package com.googlecode.n_orm.sample.businessmodel;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.googlecode.n_orm.PersistingMixin;
import com.googlecode.n_orm.PropertyManagement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.hbase.Store;

public class HBaseTest extends BasicTest {
	static {
		//Running the launcher if necessary
		@SuppressWarnings("unused")
		Class<?> c = HBaseTestLauncher.class;
	}
	 
	 @Test
	 public void compression() throws IOException {
		//src/test/resources/com/googlecode/n_orm/sample/businessmodel/store.properties defines the HBase store
		com.googlecode.n_orm.storeapi.Store ast = StoreSelector.getInstance().getStoreFor(Book.class);
		if (! (ast instanceof Store)) //In case you've changed the default store
			return;
		Store st = (Store) ast;
		//According to src/test/resources/com/googlecode/n_orm/sample/businessmodel/store.properties, can be LZO (if available) or GZ
		assertTrue(st.getCompression().equals("lzo") || st.getCompression().equals("gz"));
		//Checking with HBase that the property column family is actually stored in GZ
		HTableDescriptor td = st.getAdmin().getTableDescriptor(Bytes.toBytes(PersistingMixin.getInstance().getTable(Book.class) /*could be bsut.getTable()*/));
		//Getting the property CF descriptor
		HColumnDescriptor pd = td.getFamily(Bytes.toBytes(PropertyManagement.PROPERTY_COLUMNFAMILY_NAME));
		//According to src/test/resources/com/googlecode/n_orm/sample/businessmodel/store.properties, can be LZO (if available) or GZ
		Algorithm comp = pd.getCompression();
		assertTrue(comp.equals(Algorithm.LZO) || comp.equals(Algorithm.GZ));
	 }
}
