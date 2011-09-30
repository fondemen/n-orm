package com.googlecode.n_orm.hbase;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
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
			File clFile = new File(clFilename), file;
			try {
			  file = new File(res.toURI());
			} catch(URISyntaxException e) {
			  file = new File(res.getPath());
			}
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

	@Test
	public void loadPropertiesWithIgnore() throws IOException {
		Configuration conf = Store.getStore(testPath + "/etc/conf1/,!" + testPath + "/etc/conf2," + testPath + "/etc/conf2/conf3/").getConf();
		assertEquals("dummyval", conf.get("dummy.prop.one"));
		assertEquals("2", conf.get("dummy.prop.two"));
		assertEquals("dummyval12", conf.get("dummy12.prop"));
		assertNull(conf.get("dummy2.prop"));
	}

	@Test(expected=IOException.class)
	public void loadPropertiesMissingHbaseSite() throws IOException {
		Store.getStore(testPath+"/etc/conf1/");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesRecursiveFindHbaseSite() throws IOException {
		Store.getStore(testPath + "/etc/conf1/," + testPath + "/etc/conf2");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesBadFolder() throws IOException {
		Store.getStore(testPath + "/etcx/conf1/," + testPath + "/etc/conf2," + testPath + "/etc/conf2/conf3/");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesNoConf() throws IOException {
		Store.getStore(testPath+"/etc/conf4");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcard() throws IOException {
		Store.getStore(testPath + "/etc/conf2/conf3/,*.xml");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcardForDir() throws IOException {
		Store.getStore(testPath + "/etc/,**/conf*/*/*.xml");
	}

	@Test(expected=Exception.class)
	public void loadPropertiesWithWildcardForDirBadSubdir() throws IOException {
		Store.getStore(testPath + "/etc/,**/conf*/toto/*.xml");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForSubdir4() throws IOException {
		Store.getStore(testPath + "/etc/,**/*4/*.xml");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcardForSubdir2Only() throws IOException {
		Store.getStore(testPath + "/etc/,**/*2/**");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForSubdir2ExcludingXML() throws IOException {
		Store.getStore(testPath + "/etc/,**/*2/**,!*.xml");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcardForSubdir2ExcludingJPG() throws IOException {
		Store.getStore(testPath + "/etc/,**/*2/**,!*.jpg");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForSubdir2ExcludingConf3() throws IOException {
		Store.getStore(testPath + "/etc/,**/*2/**,!*3");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForSubdir2ExcludingConf3XML() throws IOException {
		Store.getStore(testPath + "/etc/,*2,!*3/*.xml");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcardForSubdir2ExcludingConf3JPG() throws IOException {
		Store.getStore(testPath + "/etc/,*2,!*3/*.jpg");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForOneDir() throws IOException {
		Store.getStore(testPath + "/etc/,/*/hbase-site.xml");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcardForMultibleSubdir() throws IOException {
		Store.getStore(testPath + "/etc/,**/hbase-site.xml");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForMultibleSubdirFilered() throws IOException {
		Store.getStore(testPath + "/etc/,**/hbase-site.xml,!hbase*.xml");
	}

	@Test(expected=IOException.class)
	public void loadPropertiesWithWildcardForMultibleSubdirFileredSub() throws IOException {
		Store.getStore(testPath + "/etc/,**/hbase-site.xml,!*3/*.xml");
	}

	@Test(expected=Test.None.class)
	public void loadPropertiesWithWildcardForMultibleSubdirFileredSubJPG() throws IOException {
		Store.getStore(testPath + "/etc/,**/hbase-site.xml,!*3/*.jpg");
	}

	@Test(expected=Test.None.class)
	public void rgygtyhthntnthtntu() throws IOException {
		Store.getStore("/usr/lib/hbase/conf,/usr/lib/hadoop/conf");
	}
}
