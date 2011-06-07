package com.googlecode.n_orm.sample.businessmodel;

import java.io.IOException;
import java.util.Properties;

import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.hbase.HBaseLauncher;
import com.googlecode.n_orm.hbase.Store;

public class HBaseTestLauncher {
	//The following is unecessary for production code: all information is available in the nearest store.properties
	// (here in src/test/resources/com/googlecode/n_orm/sample/store.properties found from the classpath)
	static { //Launching a test HBase data store if unavailable
		Properties p;
		try {
			//Finding properties for our sample classes
			p = StoreSelector.getInstance().findProperties(BookStore.class);
			assert Store.class.getName().equals(p.getProperty("class"));
			//Setting them to the test HBase instance
			HBaseLauncher.hbaseHost = p.getProperty("1");
	        HBaseLauncher.hbasePort = Integer.parseInt(p.getProperty("2"));
	        Store store = Store.getStore(HBaseLauncher.hbaseHost, HBaseLauncher.hbasePort);
	        HBaseLauncher.prepareHBase();
	        if (HBaseLauncher.hBaseServer != null) {
	        		//Cheating the actual store for it to point the test data store
	                HBaseLauncher.setConf(store);
	        }
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
}
