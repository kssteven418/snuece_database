

//////////////////////////////////////////////////////////////////////////////////////
///////////////////// NODE LIST :  STORES COMMENT PARSING RESULT /////////////////////
//////////////////////////////////////////////////////////////////////////////////////


import java.util.ArrayList;
import java.util.List;

class TableAttr{
	String table;
	String attr;
	
	TableAttr(){
		table = "";
		attr = "";
	}
	
	TableAttr(String t, String a){
		table = t;
		attr = a;
	}
	
	boolean equals(TableAttr ta) {
		return ((table.equals(ta.table)) && attr.equals(ta.attr));
	}
}

/* Node_data stores 'table.attr' or 'table.attr op value' */
class Query {
	
	int op_type;
	// 1 : SELECT : table and attr only  i.e. S.sid
	// 2 : FROM : table only  i.e. S
	// 3 : WHERE w/ value : table, attr, operation, and value  i.e. S.sid = 10
	// 4 : WHERE w/ attr : table, attr, operation, table2, and attr2  i.e. S.sid = R.sid
	
	
	
	TableAttr ta = new TableAttr();
	char operation; /* =, <, > only */
	String value;
	TableAttr ta2 = new TableAttr();

	// SELECT or ORDER BY
	Query(String tn, String an) {
		op_type = 1;
		ta = new TableAttr(tn, an);
	}
	
	// FROM
	Query(String tn) {
		op_type = 2;
		ta = new TableAttr(tn, "");
	}
	
	// WHERE w/ value
	Query(String tn, String an, char op, String val) {
		op_type = 3;
		ta = new TableAttr(tn, an);
		operation = op;
		value = val;
	}
	
	// WHERE w/ attr
	Query(String tn, String an, char op, String tn2, String an2) {
		op_type = 4;
		ta = new TableAttr(tn, an);
		operation = op;
		ta2 = new TableAttr(tn2, an2);
	}
	
	void print() {
		
		String table = ta.table;
		String attr = ta.attr;
		String table2= ta2.table;
		String attr2 = ta2.attr;
		
		switch(op_type) {
		case 1: System.out.print("[" + table +", " + attr +"]"); return;
		case 2: System.out.print("[" + table + "]"); return;
		case 3: System.out.print("[" + table +", " + attr +", " + operation +", " + value + "]"); return;
		case 4: System.out.print("[" + table +", " + attr +", " + operation +", " + table2 +", " + attr2 + "]"); return;
		default : System.out.print("NA"); return;
		}
	}

}



/* Node List stores multiple node_data in a list */
class QueryList {
	
	int op_type;
	// 1 : SELECT 
	// 2 : FROM 
	// 3 : WHERE (either with value or attr)
	// 4 : ORDERBY
	
	List<Query> list;

	public QueryList() {
		list = new ArrayList<Query>();
	}

	public void insert(Query q) {
		list.add(q);
	}
	
	Query get(int ind) {
		return list.get(ind);
	}
	
	String op() {
		switch(op_type) {
		case 1: return "SELECT";
		case 2: return "FROM";
		case 3: return "WHERE";
		case 4: return "ORDER BY";
		default: return "UNKNOWN";
		}
	}

	public void print() {
		System.out.print(op() + " Query List : ");
		for(Query q:list) {
			q.print();
			System.out.print(" ");
		}
		System.out.println();
	}
}

