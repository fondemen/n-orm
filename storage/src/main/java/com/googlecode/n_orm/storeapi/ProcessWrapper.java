package com.googlecode.n_orm.storeapi;

import java.util.Set;

import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.Process;
import com.googlecode.n_orm.StorageManagement;

public class ProcessWrapper<AE extends PersistingElement, E extends AE> implements Process<Row> {
	private static final long serialVersionUID = 108311129419175780L;
	private final Process<AE> process;
	private final Class<E> clazz;
	private final Set<String> toBeActivated;
	
	public ProcessWrapper(Process<AE> process, Class<E> clazz,
			Set<String> toBeActivated) {
		super();
		this.process = process;
		this.clazz = clazz;
		this.toBeActivated = toBeActivated;
	}

	public Process<AE> getActualProcess() {
		return process;
	}

	@Override
	public void process(Row row) throws Throwable {
		process.process((AE)StorageManagement.getFromRawData(this.clazz, row, this.toBeActivated));
	}
	
}
