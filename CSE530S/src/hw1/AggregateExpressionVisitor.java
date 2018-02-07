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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;

/**
 * Processes aggregate functions using the visitor pattern
 * @author Doug Shook
 *
 */
public class AggregateExpressionVisitor extends ExpressionVisitorAdapter {

	private AggregateOperator op;
	private String column;
	private boolean isAggregate;
	
	public AggregateExpressionVisitor() {
		column = "";
		isAggregate = false;
		op = null;
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		column = "*";
	}
	
	@Override
	public void visit(Column column) {
		this.column = column.getColumnName();
	}
	
	@Override
	public void visit(Function function) {
		String f = function.getName();
		f = f.toLowerCase();
		switch (f) {
		case "avg":
			op = AggregateOperator.AVG;
			break;
		case "count":
			op = AggregateOperator.COUNT;
			break;
		case "min":
			op = AggregateOperator.MIN;
			break;
		case "max":
			op = AggregateOperator.MAX;
			break;
		case "sum":
			op = AggregateOperator.SUM;
			break;
		default:
			throw new UnsupportedOperationException("Aggregate Functions only");
		
		}
		
		List<Expression> el = function.getParameters().getExpressions();
		if(el.size() > 1) {
			throw new UnsupportedOperationException("Aggregate Functions only");
		} else {
			column = el.get(0).toString();
		}
		this.isAggregate = true;
	}
	
	public String getColumn() {
		return this.column;
	}
	
	public AggregateOperator getOp() {
		return this.op;
	}
	
	public boolean isAggregate() {
		return this.isAggregate;
	}
}
