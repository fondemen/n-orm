package com.googlecode.n_orm.hbase.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.conversion.ConversionTools;
import com.googlecode.n_orm.hbase.RowWrapper;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.storeapi.ProcessWrapper;
import com.googlecode.n_orm.storeapi.Row;

public class ActionJob {

	static final String NAME = "n-orm.process-runner";
	private static final String FAMILIES_TO_BE_ACTIVATED_PROP = NAME + ".familiesToBeActivated";
	private static final String ELEMENT_CLASS_PROP = NAME + ".elementClass";
	private static final String PROCESS_PROP = NAME + ".process";

	public static class ActionMapper extends
			TableMapper<ImmutableBytesWritable, Result> {
		private ProcessWrapper<?, ?> process;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				String ecn = conf.get(ELEMENT_CLASS_PROP);
				Class<?> elementClass = ClassLoader.getSystemClassLoader().loadClass(ecn);
				
				byte[] propRep = Base64.decodeBase64(conf.get(PROCESS_PROP));
				ByteArrayInputStream pbis = new ByteArrayInputStream(propRep);
				ObjectInputStream pois = new ObjectInputStream(pbis);
				Process<?> p = (Process<?>) pois.readObject();
				pois.close(); pbis.close();
				
				String[] families = conf.getStrings(FAMILIES_TO_BE_ACTIVATED_PROP);
				Set<String> fams = new TreeSet<String>(Arrays.asList(families));
				this.process = new ProcessWrapper(p, elementClass, fams);
			} catch (RuntimeException e) {
				throw e;
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		protected void map(final ImmutableBytesWritable key, final Result value,
				Context context) throws IOException, InterruptedException {
			this.process.process(new RowWrapper(value));
		}
	}

	public static Job createSubmittableJob(Store s, String tableName,
			Scan scan, Process<? extends PersistingElement> process, Class<? extends PersistingElement> elementClass, String[] families) throws IOException {
		Class<?> processClass = process.getClass();
		
		Configuration conf = LocalFormat.prepareConf(s, null);
		conf.set(ELEMENT_CLASS_PROP, elementClass.getName());
		conf.setStrings(FAMILIES_TO_BE_ACTIVATED_PROP, families);
		//Serializing process
		ByteArrayOutputStream pobs = new ByteArrayOutputStream();
		ObjectOutputStream poos = new ObjectOutputStream(pobs);
		poos.writeObject(process); poos.flush();
		conf.set(PROCESS_PROP, Base64.encodeBase64String(pobs.toByteArray()));
		poos.close();pobs.close();
		
		Job job = new Job(conf, NAME + "_" + processClass.getName() + "(" + elementClass.getName() + ")_" + scan.hashCode());
		job.setJarByClass(processClass);
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(tableName, scan,
				ActionMapper.class, ImmutableBytesWritable.class,
				Result.class, job, true);
		LocalFormat.prepareJob(job);
		job.setNumReduceTasks(0);
		return job;
	}
}
