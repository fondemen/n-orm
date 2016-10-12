package com.googlecode.n_orm.console.util;

import org.apache.commons.beanutils.Converter;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by laurent@beampulse.com on 04/03/16.
 */

public class EnumConverter<T> implements Converter {

	private Class<T> clazz;
	private String[] values;

	public EnumConverter(Class<T> clazz) {
		this.clazz = clazz;
	}

	@Override
	public Object convert(Class type, Object value) {

		try {
			Method valueOf = type.getMethod("valueOf", String.class);
			return valueOf.invoke(null, value);
		} catch (Exception e) {
			// handle error here
			throw new IllegalArgumentException(type.getSimpleName() + " has no value: " + value);
		}
	}

	public String[] getValues() {
		if (this.values == null) {
			List<String> enumValues = new ArrayList<String>();

			for (Object o : clazz.getEnumConstants()) {
				enumValues.add(o.toString());
			}

			this.values = enumValues.toArray(new String[0]);
		}

		return this.values;
	}
}
