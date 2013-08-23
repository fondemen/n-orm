package com.googlecode.n_orm.hbase.properties;

import java.lang.annotation.Annotation;

import com.googlecode.n_orm.hbase.HBaseSchema;

public class DefaultHBaseSchema implements HBaseSchema {
	@HBaseSchema
	static class DummyClass {
	}

	private static HBaseSchema defaultHbaseSchema;

	static {
		defaultHbaseSchema = DummyClass.class.getAnnotation(HBaseSchema.class);
	}

	private Class<? extends Annotation> annotationType = defaultHbaseSchema
			.annotationType();
	private SettableBoolean forceCompression = defaultHbaseSchema
			.forceCompression();
	private String compression = defaultHbaseSchema.compression();
	private int scanCaching = defaultHbaseSchema.scanCaching();
	private SettableBoolean forceInMemory = defaultHbaseSchema.forceInMemory();
	private SettableBoolean inMemory = defaultHbaseSchema.inMemory();
	private SettableBoolean forceTimeToLive = defaultHbaseSchema
			.forceTimeToLive();
	private int timeToLiveInSeconds = defaultHbaseSchema.timeToLiveInSeconds();
	private SettableBoolean forceMaxVersions = defaultHbaseSchema
			.forceMaxVersions();
	private int maxVersions = defaultHbaseSchema.maxVersions();
	private SettableBoolean forceBloomFilterType = defaultHbaseSchema
			.forceBloomFilterType();
	private String bloomFilterType = defaultHbaseSchema.bloomFilterType();
	private SettableBoolean forceBlockCacheEnabled = defaultHbaseSchema
			.forceBlockCacheEnabled();
	private SettableBoolean blockCacheEnabled = defaultHbaseSchema
			.blockCacheEnabled();
	private SettableBoolean forceBlockSize = defaultHbaseSchema
			.forceBlockSize();
	private int blockSize = defaultHbaseSchema.blockSize();
	private SettableBoolean forceReplicationScope = defaultHbaseSchema
			.forceReplicationScope();
	private int replicationScope = defaultHbaseSchema.replicationScope();
	private SettableBoolean deferredLogFlush = defaultHbaseSchema
			.deferredLogFlush();
	private SettableBoolean forceDeferredLogFlush = defaultHbaseSchema
			.forceDeferredLogFlush();
	private WALWritePolicy writeToWAL = defaultHbaseSchema.writeToWAL();

	@Override
	public Class<? extends Annotation> annotationType() {
		return this.annotationType;
	}

	@Override
	public SettableBoolean forceCompression() {
		return this.forceCompression;
	}

	@Override
	public String compression() {
		return this.compression;
	}

	@Override
	public int scanCaching() {
		return this.scanCaching;
	}

	@Override
	public SettableBoolean forceInMemory() {
		return this.forceInMemory;
	}

	@Override
	public SettableBoolean inMemory() {
		return this.inMemory;
	}

	@Override
	public SettableBoolean forceTimeToLive() {
		return this.forceTimeToLive;
	}

	@Override
	public int timeToLiveInSeconds() {
		return this.timeToLiveInSeconds;
	}

	@Override
	public SettableBoolean forceMaxVersions() {
		return this.forceMaxVersions;
	}

	@Override
	public int maxVersions() {
		return this.maxVersions;
	}

	@Override
	public SettableBoolean forceBloomFilterType() {
		return this.forceBloomFilterType;
	}

	@Override
	public String bloomFilterType() {
		return this.bloomFilterType;
	}

	@Override
	public SettableBoolean forceBlockCacheEnabled() {
		return this.forceBlockCacheEnabled;
	}

	@Override
	public SettableBoolean blockCacheEnabled() {
		return this.blockCacheEnabled;
	}

	@Override
	public SettableBoolean forceBlockSize() {
		return this.forceBlockSize;
	}

	@Override
	public int blockSize() {
		return this.blockSize;
	}

	@Override
	public SettableBoolean forceReplicationScope() {
		return this.forceReplicationScope;
	}

	@Override
	public int replicationScope() {
		return this.replicationScope;
	}

	@Override
	public SettableBoolean deferredLogFlush() {
		return this.deferredLogFlush;
	}

	@Override
	public SettableBoolean forceDeferredLogFlush() {
		return this.forceDeferredLogFlush;
	}

	@Override
	public WALWritePolicy writeToWAL() {
		return this.writeToWAL;
	}

	public void setAnnotationType(Class<? extends Annotation> annotationType) {
		this.annotationType = annotationType;
	}

	public void setForceCompression(SettableBoolean forceCompression) {
		this.forceCompression = forceCompression;
	}

	public void setCompression(String compression) {
		this.compression = compression;
	}

	public void setScanCaching(int scanCaching) {
		this.scanCaching = scanCaching;
	}

	public void setForceInMemory(SettableBoolean forceInMemory) {
		this.forceInMemory = forceInMemory;
	}

	public void setInMemory(SettableBoolean inMemory) {
		this.inMemory = inMemory;
	}

	public void setForceTimeToLive(SettableBoolean forceTimeToLive) {
		this.forceTimeToLive = forceTimeToLive;
	}

	public void setTimeToLiveInSeconds(int timeToLiveInSeconds) {
		this.timeToLiveInSeconds = timeToLiveInSeconds;
	}

	public void setForceMaxVersions(SettableBoolean forceMaxVersions) {
		this.forceMaxVersions = forceMaxVersions;
	}

	public void setMaxVersions(int maxVersions) {
		this.maxVersions = maxVersions;
	}

	public void setForceBloomFilterType(SettableBoolean forceBloomFilterType) {
		this.forceBloomFilterType = forceBloomFilterType;
	}

	public void setBloomFilterType(String bloomFilterType) {
		this.bloomFilterType = bloomFilterType;
	}

	public void setForceBlockCacheEnabled(SettableBoolean forceBlockCacheEnabled) {
		this.forceBlockCacheEnabled = forceBlockCacheEnabled;
	}

	public void setBlockCacheEnabled(SettableBoolean blockCacheEnabled) {
		this.blockCacheEnabled = blockCacheEnabled;
	}

	public void setForceBlockSize(SettableBoolean forceBlockSize) {
		this.forceBlockSize = forceBlockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public void setForceReplicationScope(SettableBoolean forceReplicationScope) {
		this.forceReplicationScope = forceReplicationScope;
	}

	public void setReplicationScope(int replicationScope) {
		this.replicationScope = replicationScope;
	}

	public void setDeferredLogFlush(SettableBoolean deferredLogFlush) {
		this.deferredLogFlush = deferredLogFlush;
	}

	public void setForceDeferredLogFlush(SettableBoolean forceDeferredLogFlush) {
		this.forceDeferredLogFlush = forceDeferredLogFlush;
	}

	public void setWriteToWAL(WALWritePolicy writeToWAL) {
		this.writeToWAL = writeToWAL;
	}

}