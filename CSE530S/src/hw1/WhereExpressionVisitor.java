/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/**
 * Processes where clauses using the visitor pattern
 * @author Doug Shook
 *
 */
public class WhereExpressionVisitor extends ExpressionVisitorAdapter {
	
	private RelationalOperator op;
	private String left;
	private Field right;
	
	@Override
	public void visit(EqualsTo equalsTo) {
		op = RelationalOperator.EQ;
		processOps(equalsTo);
	}
	
	@Override
	public void visit(GreaterThan greaterThan) {
		op = RelationalOperator.GT;
		processOps(greaterThan);
	}
	
	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		op = RelationalOperator.GTE;
		processOps(greaterThanEquals);
	}
	
	@Override
	public void visit(MinorThan minorThan) {
		op = RelationalOperator.LT;
		processOps(minorThan);
	}
	
	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		op = RelationalOperator.LTE;
		processOps(minorThanEquals);
	}
	
	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		op = RelationalOperator.NOTEQ;
		processOps(notEqualsTo);
	}
	
	private void processOps(ComparisonOperator c) {
		left = ((Column)c.getLeftExpression()).getColumnName();
		String r = c.getRightExpression().toString();
		
		try {
			right = new IntField(Integer.parseInt(r));
		} catch (NumberFormatException e) {
			right = new StringField(r);
		}
	}
	
	public String getLeft() {
		return left;
	}
	
	public Field getRight() {
		return right;
	}
	
	public RelationalOperator getOp() {
		return op;
	}
}
