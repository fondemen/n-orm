package com.googlecode.n_orm.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.googlecode.n_orm.hbase.Store;


public class HBaseLoaderTest {
	private String testPath = null;
	
	@Before
	public void getResourcePath() {
		if (testPath == null) {
			String clFilename = HBaseLoaderTest.class.getName().replace('.', '/')+".class";
			URL res = ClassLoader.getSystemClassLoader().getResource(clFilename);
			File clFile = new File(clFilename);
			File file = new File(res.getPath());
			do {
				clFile = clFile.getParentFile();
				file = file.getParentFile();
			} while (clFile != null);
			testPath = file.getAbsolutePath();
		}
	}

	@Test
	public void loadProperties() throws IOException {
		Configuration conf = Store.getStore(testPath + "/etc/conf1/," + testPath + "/etc/conf2," + testPath + "/etc/conf2/conf3/").getConf();
		assertEquals("dummyval", conf.get("dummy.prop.one"));
		assertEquals("2", conf.get("dummy.prop.two"));
		assertEquals("dummyval12", conf.get("dummy12.prop"));
		assertEquals("dummy2val", conf.get("dummy2.prop"));
	}

	@Test(expected=IOException.class)
	public void loadPropertiesMissingHbaseSite() throws IOException {
		Store.getStore(testPath + "/etc/conf1/," + testPath + "/etc/conf2");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesBadFolder() throws IOException {
		Store.getStore(testPath + "/etcx/conf1/," + testPath + "/etc/conf2," + testPath + "/etc/conf2/conf3/");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesNoConf() throws IOException {
		Store.getStore(testPath + "/etc/conf1/," + testPath + "/etc/conf4," + testPath);
	}
}
