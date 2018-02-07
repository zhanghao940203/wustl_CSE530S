package hw3;


import java.awt.RenderingHints.Key;
import java.security.cert.PKIXRevocationChecker.Option;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;

import hw1.Field;
import hw1.IntField;
import hw1.RelationalOperator;

public class BPlusTree {
    int degree;
    Node root;
    public BPlusTree(int degree) {
    	//your code here
    	this.degree=degree;
    	root=null;
    }
    
    public LeafNode search(Field f) {
    	//your code here
    	if(root==null||f==null){
    		return null;
    	}
    	LeafNode node=(LeafNode)searchLeaf(root, f);
		for(int i=0; i<node.entries.size(); i++) {
			if(f.compare(RelationalOperator.EQ, node.entries.get(i).getField())){
				return node;
			}
		}
		return null;
    		
    } 
    public LeafNode searchforLeaf(Field f) {
    	//your code here
    	if(root==null||f==null){
    		return null;
    	}
    	LeafNode node=(LeafNode)searchLeaf(root, f);

		return node;
    		
    }
    public LeafNode searchLeaf(Node croot, Field f){
    	if(croot.isLeafNode()){
    		return (LeafNode) croot;
    	}else{
    		InnerNode innerNode=(InnerNode)croot;
    		if(f.compare(RelationalOperator.LTE, innerNode.keys.get(0))){
    			return searchLeaf(innerNode.getChildren().get(0), f);
    			
    		}
    		else if(f.compare(RelationalOperator.GT, innerNode.keys.get(innerNode.keys.size()-1))){
    			return searchLeaf(innerNode.getChildren().get(innerNode.keys.size()-1), f);
    		}
    		else{
    			for(int i=0; i<innerNode.keys.size(); i++) {
    				if(f.compare(RelationalOperator.GT, innerNode.keys.get(i))&&f.compare(RelationalOperator.LTE, innerNode.keys.get(i+1))){
    					return searchLeaf(innerNode.getChildren().get(i+1), f);
    				}
				}
    		}
    		return null;
    	}
    }    
 // insert --------------------------------------------------------------------------------
    public void insert(Entry e) {
    	//your code here
    	if(root==null){
			    		root=new LeafNode(degree);
			    		LeafNode node=new LeafNode(degree);
			    		node.entries.add(e);
			    		setRoot(node);
    	}else{
			    	LeafNode nodeToInsert=(LeafNode)searchforLeaf(e.getField());
			    	//if an insertion is requested for a value that is already in the tree, the tree should not change.
			    	for(int i=0; i<nodeToInsert.entries.size(); i++) {
						if(e.getField().compare(RelationalOperator.EQ, nodeToInsert.entries.get(i).getField())){
							return;
						}
					}
			    	int sizeOfnodeToInsert=nodeToInsert.entries.size();
			    	if(nodeToInsert.entries.size()<degree){
				    		if(nodeToInsert.entries.size()==0){
				    			nodeToInsert.entries.add(e);
				    			
				    		}else{
				    			for(int i=0;i<nodeToInsert.entries.size();i++){
				    				if(e.getField().compare(RelationalOperator.GT, nodeToInsert.entries.get(sizeOfnodeToInsert-1).getField())){
				    					nodeToInsert.entries.add(e);
				    					break;
				    				}
				       				if(e.getField().compare(RelationalOperator.LT, nodeToInsert.entries.get(i).getField())){
				    					nodeToInsert.entries.add(i,e);
				    					break;
				    				}
				    			}
				    			
				    		}    		
				    }else{
				// complex insert:---------------------------------------------------------------------------------
		//		    		LeafNode temp=nodeToInsert.next;
				    	
				    		ArrayList<LeafNode> list=null;
				    		InnerNode parent=nodeToInsert.getParents();
				    		int index=0;
				    		ArrayList<Node> childrenOfparent=null;
				    		 
				    		if(parent!=null){
				    			index=parent.childrens.indexOf(nodeToInsert);
				    			parent.childrens.remove(nodeToInsert);
				    			childrenOfparent=parent.childrens;
				    		}
				    		LeafNode tempNodeToInsert=nodeToInsert;
				    		list=splitAndAddNodes(nodeToInsert, e);
			    			
			    			nodeToInsert=list.get(0);
			    			LeafNode newNode=list.get(1);
			    			
			    			nodeToInsert.setParents(parent);			    			
			    			newNode.setParents(parent);
//			    			if(parent!=null){
//			    				parent.childrens.add(index,nodeToInsert);
//				    			parent.childrens.add(index+1,newNode);
//			    			}
			    			
			    			Field aField=nodeToInsert.entries.get(nodeToInsert.entries.size()-1).getField();
			    			System.out.println("dajsdashdlkashdkl"+aField);
			    			while(parent!=null){
				    				if(parent.keys.size()<degree){
					    					for(int k=0;k<parent.keys.size();k++){
					    	        			if(aField.compare(RelationalOperator.GT, parent.keys.get(parent.keys.size()-1))){
					    	        				parent.keys.add(aField);
					    	        				parent.childrens.add(index,nodeToInsert);
									    			parent.childrens.add(index+1,newNode);
					    	        				break;
					    	        			}
					    	        			if(aField.compare(RelationalOperator.LT, parent.keys.get(k))){	
					    	        				parent.keys.add(k,aField);
					    	        				
					    	        				parent.childrens.add(index,nodeToInsert);
									    			parent.childrens.add(index+1,newNode);
					    	        				break;
					    	        			}
					    	        		}
					    				break;
				    				}
				    				if(parent.keys.size()>=degree){
					    					Field selectedField=null;	    	
		//			    					ArrayList<Node> childrenOfparent=parent.childrens;
					    					InnerNode parentOfParent=parent.parents;
					    					InnerNode tempo=parent;
		//			    					System.out.println("hahaha"+parent.keys.get(0));
					    					int number=0;
					    					if(parentOfParent!=null){
					    	        		
					    	        			number=parentOfParent.childrens.indexOf(tempo);
					    	        			parentOfParent.childrens.remove(number);
					    	        			
					    	        		}	
					    	        		ArrayList<InnerNode> list1=splitAndAddInnerNodes(parent, aField);
					    	        		parent=list1.get(0);
					    	        		InnerNode rightParent=list1.get(1);		   		    	     
					    	        		
					    	        		if(parentOfParent!=null){
					    	        			parent.setParents(parentOfParent);
						    	        		rightParent.setParents(parentOfParent);		    	        			
					    	        			parentOfParent.childrens.add(number,parent);
						    	        		parentOfParent.childrens.add(number+1,rightParent);
					    	        		}			    	        					    		
					    	        		selectedField=parent.keys.get(parent.keys.size()-1);			    	        		
					    	        		System.out.println("sssss"+selectedField);
					    	        		parent.keys.remove(parent.keys.size()-1);
					    	        		System.out.println("llllll"+parent.keys.get(0));
					    	        		System.out.println("rrrrrr"+rightParent.keys.get(parent.keys.size()-1));
					    	        		aField=selectedField;
					    	        		if(index<degree/2){
					    	        			parent.childrens.add(nodeToInsert);
					    	        			parent.childrens.add(newNode);		    	        			
					    	        			for(int i=0;i<childrenOfparent.size();i++){
					    	        				rightParent.childrens.add(childrenOfparent.get(i));
					    	        				childrenOfparent.get(i).setParents(rightParent);
					    	        			}
					    	        			nodeToInsert.setParents(parent);
					    	        			newNode.setParents(parent);
					    	        		}
					    	        		if(index==degree/2){
					    	        			parent.childrens.add(nodeToInsert);
					    	        			rightParent.childrens.add(newNode);
					    	        			for(int i=0;i<childrenOfparent.size()/2;i++){
					    	        				parent.childrens.add(childrenOfparent.get(i));
					    	        				childrenOfparent.get(i).setParents(parent);
					    	        			}
					    	        			for(int i=childrenOfparent.size()/2;i<childrenOfparent.size();i++){
					    	        				rightParent.childrens.add(childrenOfparent.get(i));
					    	        				childrenOfparent.get(i).setParents(rightParent);
					    	        			}
					    	        			nodeToInsert.setParents(parent);
					    	        			newNode.setParents(rightParent);
					    	        		}
					    	        		if(index>degree/2){
					    	        			rightParent.childrens.add(nodeToInsert);
					    	        			rightParent.childrens.add(newNode);
					    	        			for(int i=0;i<childrenOfparent.size();i++){
					    	        				parent.childrens.add(childrenOfparent.get(i));
					    	        				childrenOfparent.get(i).setParents(parent);
					    	        			}
					    	        			nodeToInsert.setParents(rightParent);
					    	        			newNode.setParents(rightParent);
					    	        			
					    	        		}					    	     	
					    	        		InnerNode tempParent=parent.parents;	
					    	        		
					    	        		if(tempParent==null){
					    	        			
					    	        			tempParent=new InnerNode(index);
					    	        			tempParent.keys.add(aField);
					    	        			setRoot(tempParent);
					    	        			parent.setParents(tempParent);
					    	        			rightParent.setParents(tempParent);
					    	        			tempParent.childrens.add(parent);
					    	        			tempParent.childrens.add(rightParent);
					    	        			parent=tempParent;
					    	        			break;
					    	        		}
					    	        		parent=parent.parents;	
					    	        		System.out.println("dhjiashgdjkasbdkjasgdkja"+parent.keys.get(0));
					    	        		System.out.println("00000!!!"+(((InnerNode)parent.childrens.get(0)).keys.get(0)));
				    				}
				    				
			    			}
			    			if(parent==null){
			    				parent=new InnerNode(degree);
			    	    		parent.keys.add(aField);
			    	    		setRoot(parent);
			    	    		nodeToInsert.setParents(parent);
			    	    		newNode.setParents(parent);
			    	    		parent.childrens.add(nodeToInsert);
			    	    		parent.childrens.add(newNode);
			    			}
		
			    			
				    			
				    }//for complex insert
			    	
    		}
    	
    }

 //  --------------------------------------------------------------------------------
    
 
//    ADD INTO PARENTS:--------------------------------------------------------------------------------
//    public void addIntoParents(InnerNode parent,Field fieldToBeInserted,int degree,ArrayList<Node> child){
//    	
//    	if(parent==null){
//    		parent=new InnerNode(degree);
//    		parent.keys.add(fieldToBeInserted);
//    		for(int i=0;i<child.size();i++){
//    			child.get(i).setParents(parent);
//    			parent.childrens.add(child.get(i));
//    		}
//    		this.root=parent;
//    		return;
//    	}else{
//    		if(parent.keys.size()<degree){
//        		for(int i=0;i<child.size();i++){
//        			child.get(i).setParents(parent);
//        			parent.childrens.add(child.get(i));
//        		}
//        		for(int k=0;k<parent.keys.size();k++){
//        			if(((IntField)(fieldToBeInserted)).getValue()>((IntField)(parent.keys.get(parent.keys.size()-1))).getValue()){
//        				parent.keys.add(fieldToBeInserted);
//        				this.root=parent;
//        				break;
//        			}
//        			if(((IntField)(fieldToBeInserted)).getValue()<((IntField)(parent.keys.get(k))).getValue()){
//        				parent.keys.add(k,fieldToBeInserted);
//        				this.root=parent;
//        				break;
//        			}
//        		}
//        		return;
//        	}
//        	if(parent.keys.size()>=degree){
//        		for(int i=0;i<child.size();i++){
//        			child.get(i).setParents(parent);
//        			parent.childrens.add(child.get(i));
//        		}
//        		Field selectedField=null;
//        		InnerNode parentOfParent=parent.parents;
//        		ArrayList<InnerNode> list1=splitAndAddInnerNodes(parent, fieldToBeInserted);
//        		parent=list1.get(0);
//        		InnerNode newParent=list1.get(1);
//        		ArrayList<Node> childs=new ArrayList<>();
//        		selectedField=parent.keys.get(parent.keys.size()-1);
//        		parent.keys.remove(parent.keys.size()-1);
//        		childs.add(parent);
//        		childs.add(newParent);		
//        		this.root=parent;
//        		addIntoParents(parentOfParent, selectedField, degree,childs);
//        		this.root=parent;
//        		return;
//        	}
//        	
//    	}	
//    }
   // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  

    public ArrayList<LeafNode> splitAndAddNodes(LeafNode nodeToSplit,Entry e){
    	ArrayList<LeafNode> result=new ArrayList<LeafNode>();
    	for(int i=0;i<nodeToSplit.entries.size();i++){
    		if(e.getField().compare(RelationalOperator.GT, nodeToSplit.entries.get(nodeToSplit.entries.size()-1).getField())){
    			nodeToSplit.entries.add(e);
    			break;
    		}
       		if(e.getField().compare(RelationalOperator.LT, nodeToSplit.entries.get(i).getField())){
    			nodeToSplit.entries.add(i,e);
    			break;
    		}
    	}
    	LeafNode leftNode=new LeafNode(degree);
    	LeafNode rightNode=new LeafNode(degree);
    	if(nodeToSplit.entries.size()%2==0){
    		for(int i=0;i<nodeToSplit.entries.size();i++){
    			if(i<nodeToSplit.entries.size()/2){
    				leftNode.entries.add(nodeToSplit.entries.get(i));
    			}else{
    				rightNode.entries.add(nodeToSplit.entries.get(i));
    			}
    		}
    		nodeToSplit=leftNode;
    		result.add(leftNode);
    		result.add(rightNode);
    		return result;
    	}else{
    		for(int i=0;i<nodeToSplit.entries.size();i++){
    			if(i<(nodeToSplit.entries.size()/2)+1){
    				leftNode.entries.add(nodeToSplit.entries.get(i));
    			}else{
    				rightNode.entries.add(nodeToSplit.entries.get(i));
    			}
    		}
//    		nodeToSplit=leftNode;
    		result.add(leftNode);
    		result.add(rightNode);
    		return result;
    	} 	
    	
    }
    
    
    public ArrayList<InnerNode> splitAndAddInnerNodes(InnerNode nodeToSplit,Field f){
    	ArrayList<InnerNode> result=new ArrayList<InnerNode>();
    	for(int i=0;i<nodeToSplit.keys.size();i++){
    		if(f.compare(RelationalOperator.GT, nodeToSplit.keys.get(nodeToSplit.keys.size()-1))){
    			nodeToSplit.keys.add(f);
    			break;
    		}
       		if(f.compare(RelationalOperator.LT, nodeToSplit.keys.get(i))){
    			nodeToSplit.keys.add(i,f);
    			break;
    		}
    	}
    	InnerNode leftNode=new InnerNode(degree);
    	InnerNode rightNode=new InnerNode(degree);
    	Field fieldToBePoped=null;
    	
    	if(nodeToSplit.keys.size()%2==0){
    		for(int i=0;i<nodeToSplit.keys.size();i++){
    			if(i<nodeToSplit.keys.size()/2){
    				leftNode.keys.add(nodeToSplit.keys.get(i));
    			}else{
    				rightNode.keys.add(nodeToSplit.keys.get(i));
    			}
    		}
    		nodeToSplit=leftNode;
    		result.add(leftNode);
    		result.add(rightNode);
    		return result;
    	}else{
    		for(int i=0;i<nodeToSplit.keys.size();i++){
    			if(i<(nodeToSplit.keys.size()/2)+1){
    				leftNode.keys.add(nodeToSplit.keys.get(i));
    			}else{
    				rightNode.keys.add(nodeToSplit.keys.get(i));
    			}
    		}
    		nodeToSplit=leftNode;
    		result.add(leftNode);
    		result.add(rightNode);
    		return result;
    	} 	
    	
    } 
    public void delete(Entry e) {
    	//your code here
    	//If a deletion is requested for a value that is not in the tree, simply do nothing.
    	if(search(e.getField())==null){
    		return;
    	}
    	LeafNode nodeTodelete=search(e.getField());
    	int sizeOfnodeTodelete=nodeTodelete.entries.size();
    	if(sizeOfnodeTodelete>degree/2){
    		nodeTodelete.entries.remove(e.getField());
    	}else{
    		
    	}
    }
    
    public Node getRoot() {
    	//your code here
    	return root;
    }
    public void setRoot(Node n){
    	this.root=n;
    }
}
