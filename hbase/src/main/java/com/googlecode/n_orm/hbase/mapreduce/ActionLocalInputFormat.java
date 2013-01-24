package com.googlecode.n_orm.hbase.mapreduce;

import java.io.StringReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.googlecode.n_orm.hbase.Store;

public class ActionLocalInputFormat extends LocalInputFormat {

	@Override
	public void setConf(Configuration conf) {
		//Starts a store using this configuration
		try {
			Properties props = null;
			StringReader srp = new StringReader(conf.get(LocalFormat.STORE_REF));
			props = new Properties();
			props.load(srp);
			Store hstore = Store.getKnownStore(props);
			if (hstore == null) {
				hstore = Store.getStore(conf, props);
				hstore.setConf(conf);
				hstore.start();
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
		super.setConf(conf);
	}

}
