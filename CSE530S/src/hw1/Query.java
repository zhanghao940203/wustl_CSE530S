/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
		TablesNamesFinder tbfinder = new TablesNamesFinder();
		List<String> tbs = tbfinder.getTableList(selectStatement);
		
		List<Join> joins = sb.getJoins();
		WhereExpressionVisitor wherev = new WhereExpressionVisitor();
		Expression wheres = sb.getWhere();
		if(wheres != null){
			wheres.accept(wherev);
		}
		//System.out.println(wheres.toString());
		
		List<Expression> groupbs = sb.getGroupByColumnReferences();
		
		Catalog c = Database.getCatalog();
		if(joins == null){
			int tableid = c.getTableId(tbs.get(0));
			TupleDesc td = c.getTupleDesc(tableid);
			HeapFile hf = c.getDbFile(tableid);
			ArrayList<Tuple> tuples = hf.getAllTuples();
			Relation table1 = new Relation(tuples, td);
			
			List<SelectItem> seles = sb.getSelectItems();
			ColumnVisitor cv = new ColumnVisitor();
			int sids = 0;
			boolean agg = false;
			AggregateOperator op = null;
			ArrayList<Integer> col = new ArrayList<Integer>();
			System.out.println(seles.size());
			for(int i = 0; i < seles.size(); i++){
				seles.get(i).accept(cv);
				if(cv.isAggregate()){
					op = cv.getOp();
					String cname = cv.getColumn();
//					System.out.println(cname);
//					System.out.println(op.toString());
					col.add(td.nameToId(cname));
					agg = true;
				}
				else if(cv.getColumn() == "*"){
					for(int j = 0; j < td.numFields(); j++){
						col.add(j);
					}
				}
				else{
					String name = cv.getColumn();
					sids = td.nameToId(name);
					//System.out.println(name);
					col.add(sids);
				}
			}
			if(wheres == null){
				Relation ans = null;
				if(agg){
					ans = table1.project(col).aggregate(op, groupbs != null);
					//System.out.println(ans.getTuples().get(0).getField(0).toString());
					return ans;
				}
				return table1.project(col);
			}else{
				Relation res = table1.select(td.nameToId(wherev.getLeft()), wherev.getOp(), wherev.getRight());
				Relation ans =  res.project(col);
				if(agg){
					ans = res.aggregate(op, groupbs != null);
					return ans;
				}
				
				return ans;
			}
		}else{
			Relation ans = null;
			ArrayList<Relation> rels = new ArrayList<Relation>();
			for(int i = 0; i < tbs.size(); i++){
				int tableid = c.getTableId(tbs.get(i));
				TupleDesc td = c.getTupleDesc(tableid);
				HeapFile hf = c.getDbFile(tableid);
				ArrayList<Tuple> tuples = hf.getAllTuples();
				rels.add(new Relation(tuples, td));
			}
			List<SelectItem> seles = sb.getSelectItems();
			ColumnVisitor cv = new ColumnVisitor();
			int sids = 0;
			boolean agg = false;
			boolean all = false;
			String cname = null;
			AggregateOperator op = null;
			ArrayList<Integer> col = new ArrayList<Integer>();
			for(int i = 0; i < seles.size(); i++){
				seles.get(i).accept(cv);
				if(cv.isAggregate()){
					op = cv.getOp();
					cname = cv.getColumn();
//					System.out.println(cname);
//					System.out.println(op.toString());
					agg = true;
				}
				else if(cv.getColumn() == "*"){
					all = true;
				}
				String name = cv.getColumn();
				System.out.println(name);
				col.add(sids);
				sids++;
			}
			for(int i = 0; i < joins.size(); i++){
				//System.out.println(joins.get(i));
				String expression = joins.get(i).getOnExpression().toString().trim();
				String[] clos = expression.split("=");
				String c1 = clos[0].split("\\.")[1].trim();
				String c2 = clos[1].split("\\.")[1].trim();
				int field1 = rels.get(0).getDesc().nameToId(c1);
				int field2 = rels.get(1).getDesc().nameToId(c2);
				System.out.println(tbs.get(0));
				System.out.println(tbs.get(1));
				ans = rels.get(0).join(rels.get(1), field1, field2);
//				WhereExpressionVisitor joinv = new WhereExpressionVisitor();
//				joins.get(i).getOnExpression().accept(joinv);
//				System.out.println(joinv.getLeft());
//				System.out.println(joinv.getRight());
				//rel.join(relon, joinv.getLeft(), joinv.getRight());
			}
			
			if(wheres == null){
				if(agg){
					col.add(ans.getDesc().nameToId(cname));
					ans = ans.project(col).aggregate(op, groupbs != null);
					//System.out.println(ans.getTuples().get(0).getField(0).toString());
					return ans;
				}
				else if(all){
					for(int j = 0; j < ans.getDesc().numFields(); j++){
						col.add(j);
					}
				}
				return ans.project(col);
			}else{
				Relation res = ans.select(ans.getDesc().nameToId(wherev.getLeft()), wherev.getOp(), wherev.getRight());
				ans = res.project(col);
				if(agg){
					ans = res.aggregate(op, groupbs != null);
				}
				else if(all){
					ArrayList<Integer> col2 = new ArrayList<Integer>();
					for(int j = 0; j < ans.getDesc().numFields(); j++){
						col2.add(j);
					}
					ans = ans.project(col2);
				}
				
				return ans;
			}
			
			//return ans;
		}
		//return null;
		
	}
}
