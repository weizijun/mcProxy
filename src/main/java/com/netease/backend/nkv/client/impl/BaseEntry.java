package com.netease.backend.nkv.client.impl;

import java.util.Map.Entry;

public class BaseEntry<K, V> implements Entry<K, V> {
	private K key;
	private V value;
	
	public BaseEntry() {}
	
	public BaseEntry(K key, V value) {
		this.key = key;
		this.value = value;
	}
	
	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public K setKey(K key) {
		this.key = key;
		return key;
	}
	
	public V setValue(V value) {
		this.value = value;
		return value;
	}
}
