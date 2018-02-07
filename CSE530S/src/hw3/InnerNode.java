package hw3;

import java.util.ArrayList;

import hw1.Field;

public class InnerNode implements Node {
	int degree;
	ArrayList<Field> keys;
	ArrayList<Node> childrens;
	InnerNode parents;
	public InnerNode(int degree) {
		//your code here
		this.degree=degree;
		childrens=new ArrayList<Node>(degree+1);
		parents=null;
		keys=new ArrayList<Field>(degree);
		
	}
	
	public ArrayList<Field> getKeys() {
		//your code here
		return keys;
	}
	
	public ArrayList<Node> getChildren() {
		//your code here
		return childrens;
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
		//your code here
		return degree;
	}
	
	public boolean isLeafNode() {
		return false;
	}

	@Override
	public void setParents(Node parent) {
		// TODO Auto-generated method stub
		
	}

}