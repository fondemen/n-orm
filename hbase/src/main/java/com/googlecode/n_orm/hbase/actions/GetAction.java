package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import java.util.ArrayList;

import org.hbase.async.KeyValue;
import org.hbase.async.GetRequest;

import com.stumbleupon.async.Deferred;

public class GetAction extends Action<ArrayList<KeyValue>> {
	
	private final GetRequest get;

	public GetAction(GetRequest get) {
		super();
		this.get = get;
	}

	public GetRequest getGet() {
		return get;
	}

	@Override
	public Deferred<ArrayList<KeyValue>> perform() throws IOException {
		return this.getTable().get(this.getGet());
	}
	
}