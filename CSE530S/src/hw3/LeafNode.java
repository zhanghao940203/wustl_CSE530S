package hw3;

import java.util.ArrayList;

import hw1.Field;

public class LeafNode implements Node {
	int degree;
	ArrayList<Entry> entries;
	InnerNode parents;
	LeafNode next;
	LeafNode prev;
	public LeafNode(int degree) {
		//your code here
		this.degree=degree;
		entries=new ArrayList<Entry>(degree);
		parents=null;
		next=null;
		prev=null;
	}
	
	public ArrayList<Entry> getEntries() {
		//your code here
		return entries;
	}

	public InnerNode  getParents() {
		//your code here
		return parents;
	}
	public void  setParents(InnerNode parent) {
		//your code here
		this.parents=parent;
	}
	public int getDegree() {
		//your code herex
		return degree;
	}
	
	public boolean isLeafNode() {
		return true;
	}

	@Override
	public void setParents(Node parent) {
		// TODO Auto-generated method stub
		
	}

}