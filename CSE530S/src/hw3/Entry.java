package hw3;

import hw1.Field;

public class Entry {

	private Field f;
	private int page;
	
	public Entry(Field f, int page) {
		this.f = f;
		this.page = page;
	}
	
	public Field getField() {
		return this.f;
	}
	
	public int getPage() {
		return this.page;
	}
	
}
