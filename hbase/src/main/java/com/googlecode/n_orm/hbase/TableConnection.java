package com.googlecode.n_orm.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class TableConnection implements HTableInterface {
	public static TableConnection get(Configuration conf, MangledTableName tableName) throws IOException {
		HConnection conn = HConnectionManager.createConnection(conf);
		return new TableConnection(conn, conn.getTable(tableName.getNameAsBytes()));
	}

	private final HConnection connection;
	private final HTableInterface table;
	private TableConnection(HConnection connection, HTableInterface table) {
		super();
		this.connection = connection;
		this.table = table;
	}
	public byte[] getTableName() {
		return table.getTableName();
	}
	public TableName getName() {
		return table.getName();
	}
	public Configuration getConfiguration() {
		return table.getConfiguration();
	}
	public HTableDescriptor getTableDescriptor() throws IOException {
		return table.getTableDescriptor();
	}
	public boolean exists(Get get) throws IOException {
		return table.exists(get);
	}
	public Boolean[] exists(List<Get> gets) throws IOException {
		return table.exists(gets);
	}
	public void batch(List<? extends Row> actions, Object[] results)
			throws IOException, InterruptedException {
		table.batch(actions, results);
	}
	public Object[] batch(List<? extends Row> actions) throws IOException,
			InterruptedException {
		return table.batch(actions);
	}
	public <R> void batchCallback(List<? extends Row> actions,
			Object[] results, Callback<R> callback) throws IOException,
			InterruptedException {
		table.batchCallback(actions, results, callback);
	}
	public <R> Object[] batchCallback(List<? extends Row> actions,
			Callback<R> callback) throws IOException, InterruptedException {
		return table.batchCallback(actions, callback);
	}
	public Result get(Get get) throws IOException {
		return table.get(get);
	}
	public Result[] get(List<Get> gets) throws IOException {
		return table.get(gets);
	}
	public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
		return table.getRowOrBefore(row, family);
	}
	public ResultScanner getScanner(Scan scan) throws IOException {
		return table.getScanner(scan);
	}
	public ResultScanner getScanner(byte[] family) throws IOException {
		return table.getScanner(family);
	}
	public ResultScanner getScanner(byte[] family, byte[] qualifier)
			throws IOException {
		return table.getScanner(family, qualifier);
	}
	public void put(Put put) throws IOException {
		table.put(put);
	}
	public void put(List<Put> puts) throws IOException {
		table.put(puts);
	}
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
			byte[] value, Put put) throws IOException {
		return table.checkAndPut(row, family, qualifier, value, put);
	}
	public void delete(Delete delete) throws IOException {
		table.delete(delete);
	}
	public void delete(List<Delete> deletes) throws IOException {
		table.delete(deletes);
	}
	public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
			byte[] value, Delete delete) throws IOException {
		return table.checkAndDelete(row, family, qualifier, value, delete);
	}
	public void mutateRow(RowMutations rm) throws IOException {
		table.mutateRow(rm);
	}
	public Result append(Append append) throws IOException {
		return table.append(append);
	}
	public Result increment(Increment increment) throws IOException {
		return table.increment(increment);
	}
	public long incrementColumnValue(byte[] row, byte[] family,
			byte[] qualifier, long amount) throws IOException {
		return table.incrementColumnValue(row, family, qualifier, amount);
	}
	public long incrementColumnValue(byte[] row, byte[] family,
			byte[] qualifier, long amount, Durability durability)
			throws IOException {
		return table.incrementColumnValue(row, family, qualifier, amount,
				durability);
	}
	public long incrementColumnValue(byte[] row, byte[] family,
			byte[] qualifier, long amount, boolean writeToWAL)
			throws IOException {
		return table.incrementColumnValue(row, family, qualifier, amount,
				writeToWAL);
	}
	public boolean isAutoFlush() {
		return table.isAutoFlush();
	}
	public void flushCommits() throws IOException {
		table.flushCommits();
	}
	public void close() throws IOException {
		table.close();
		connection.close();
	}
	public CoprocessorRpcChannel coprocessorService(byte[] row) {
		return table.coprocessorService(row);
	}
	public <T extends Service, R> Map<byte[], R> coprocessorService(
			Class<T> service, byte[] startKey, byte[] endKey,
			Call<T, R> callable) throws ServiceException, Throwable {
		return table.coprocessorService(service, startKey, endKey, callable);
	}
	public <T extends Service, R> void coprocessorService(Class<T> service,
			byte[] startKey, byte[] endKey, Call<T, R> callable,
			Callback<R> callback) throws ServiceException, Throwable {
		table.coprocessorService(service, startKey, endKey, callable, callback);
	}
	public void setAutoFlush(boolean autoFlush) {
		table.setAutoFlush(autoFlush);
	}
	public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
		table.setAutoFlush(autoFlush, clearBufferOnFail);
	}
	public void setAutoFlushTo(boolean autoFlush) {
		table.setAutoFlushTo(autoFlush);
	}
	public long getWriteBufferSize() {
		return table.getWriteBufferSize();
	}
	public void setWriteBufferSize(long writeBufferSize) throws IOException {
		table.setWriteBufferSize(writeBufferSize);
	}
}
