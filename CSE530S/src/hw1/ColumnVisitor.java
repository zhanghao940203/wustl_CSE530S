/**
 * name: Hao Zhang
 * id: 452003
 * wustlkey: h.zhang633
 * name: Hanming Li
 * id: 451802
 * wustlkey: lihanming
 */
package hw1;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

/**
 * Processes SELECT clauses using the visitor pattern
 * @author Doug Shook
 *
 */
public class ColumnVisitor extends SelectItemVisitorAdapter {

	private AggregateExpressionVisitor aev;
	private String column;
	
	public ColumnVisitor() {
		aev = new AggregateExpressionVisitor();
	}
	
	@Override
	public void visit(AllColumns columns) {
		this.column = "*";
	}
	
	@Override
	public void visit(SelectExpressionItem item) {
		item.accept(aev);
		this.column = aev.getColumn();
	}
	
	public boolean isAggregate() {
		return aev.isAggregate();
	}
	
	public String getColumn() {
		return this.column;
	}
	
	public AggregateOperator getOp() {
		return this.aev.getOp();
	}
	
	
}
