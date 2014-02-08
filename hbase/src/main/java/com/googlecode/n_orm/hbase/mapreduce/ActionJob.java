package com.googlecode.n_orm.hbase.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.hbase.async.KeyValue;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.hbase.MangledTableName;
import com.googlecode.n_orm.hbase.RowWrapper;
import com.googlecode.n_orm.hbase.Store;
import com.googlecode.n_orm.hbase.actions.Scan;
import com.googlecode.n_orm.storeapi.ProcessWrapper;

public class ActionJob {

	static final String NAME = "n-orm.process-runner";
	private static final String FAMILIES_TO_BE_ACTIVATED_PROP = NAME + ".familiesToBeActivated";
	private static final String ELEMENT_CLASS_PROP = NAME + ".elementClass";
	private static final String PROCESS_PROP = NAME + ".process";

	public static class ActionMapper extends
			TableMapper<ImmutableBytesWritable, ArrayList<KeyValue>> {
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
				Set<String> fams = families == null ? new TreeSet<String>() : new TreeSet<String>(Arrays.asList(families));
				this.process = new ProcessWrapper(p, elementClass, fams);
			} catch (RuntimeException e) {
				throw e;
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}


		protected void map(final ImmutableBytesWritable key, final ArrayList<KeyValue> value,
				Context context) throws IOException, InterruptedException {
			try {
				this.process.process(new RowWrapper(value));
			} catch (RuntimeException x) {
				throw x;
			} catch (IOException x) {
				throw x;
			} catch (InterruptedException x) {
				throw x;
			} catch (Throwable x) {
				throw new RuntimeException(x);
			}
		}
	}

	public static Job createSubmittableJob(Store s, MangledTableName tableName,
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
		
		/*TableMapReduceUtil.initTableMapperJob(tableName.getNameAsBytes(), scan,
				ActionMapper.class, ImmutableBytesWritable.class,
				Result.class, job, false);
		LocalFormat.prepareJob(job, scan, s);
		if (s.isMapRedSendJobJars())
			TableMapReduceUtil.addDependencyJars(job.getConfiguration(), processClass, elementClass);
		job.setInputFormatClass(ActionLocalInputFormat.class);
		job.setNumReduceTasks(0);*/
		return job;
	}
}
