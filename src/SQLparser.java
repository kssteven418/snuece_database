/* Generated By:JavaCC: Do not edit this line. SQLparser.java */
import java.io.*;
public class SQLparser implements SQLparserConstants {

  public static Table initTable(String filepath) {

    Table table = null;
        try {
            BufferedReader in = new BufferedReader(new FileReader(filepath));
        String s;

                // read name  type from the first line
                s = in.readLine();

                String[] parse = s.split(" ");
                String[] p_name = new String[parse.length];
                String[] p_type = new String[parse.length];

                for (int i=0; i<parse.length; i++) {
                        String[] temp = parse[i].split("\u005c\u005c(|\u005c\u005c)");
                        p_name[i] = temp[0];
                        p_type[i] = temp[1];
                }

                // initiate table with the given name  type arrays
                table = new Table(p_name, p_type);

                // read line by line  insert new entry
                while ((s = in.readLine()) != null) {
                String[] temp = s.split(" ");
                table.insert(temp);
        }

        in.close();

        } catch (IOException e) {
        System.err.println(e);
    }

    return table;
  }

  public static void main(String args []) throws ParseException
  {
    SQLparser parser = new SQLparser(System.in);

        // initialize table R, S, B
        Table R = initTable("src/R.txt");
    Table S = initTable("src/S.txt");
    Table B = initTable("src/B.txt");

        TableList tables = new TableList();
        tables.insert(R, "R");
        tables.insert(S, "S");
        tables.insert(B, "B");



        /*
	//Testing table.print function and table list class

	Table temp;
	
	String[] stemp = { "temp", "R", "S", "B", "N" };
	for (int i = 0; i<stemp.length; i++) {
	  temp = tables.search(stemp[i]);
	  if(temp == null) {
	    System.out.println("No Table with name "+stemp[i]);
	    System.out.println("");
	  }
	  else {
	    System.out.println("Table with name "+stemp[i]);
	    temp.print();
	  }
	}
	*/

        /*
	//testing table copy constructor
	
	Table R_copy = new Table(R);
	System.out.println("TESTING COPY...");
	R_copy.head.next.getData()[0] = "1000";
	String[] st = { "100", "300", "10/20/30" };
	R.insert( st);
	R.print();
	R_copy.print();
	*/

    while (true)
    {
      System.out.println("Reading from stard input...");
      try
      {
        switch (SQLparser.one_line())
        {
          case 0 :
          System.out.println("OK.");
          break;
          case 1 :
          System.out.println("Goodbye.");
          break;
          default :
          break;
        }
      }
      catch (Exception e)
      {
        System.out.println("NOK.");
        System.out.println(e.getMessage());
        SQLparser.ReInit(System.in);
      }
      catch (Error e)
      {
        System.out.println("Oops.");
        System.out.println(e.getMessage());
        break;
      }
    }
  }

  static final public int one_line() throws ParseException {
  NodeList select_list;
  NodeList from_list;
  NodeList where_list;
  int output;
  Token value;
  float f;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case SELECT:
      jj_consume_token(SELECT);
      select_list = select();
      /*For debugging*/
      System.out.println("Select!");
          if(select_list == null) {
            System.out.println("*");
          }
          else {
            System.out.print("Select List : ");
            select_list.print();
          }
      jj_consume_token(FROM);
      from_list = from();
          /*For debugging*/
      System.out.println("From!");
          System.out.print("From List : ");
          from_list.print();
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case WHERE:
        jj_consume_token(WHERE);
        where_list = where();
      /*For debugging*/
      System.out.println("Where!");
          System.out.print("Where List : ");
          where_list.print();
        break;
      default:
        jj_la1[0] = jj_gen;
        ;
      }
      jj_consume_token(12);
    //Function to interpret the comm
    //by select, from, where list

        /* ***************TODO***************** */

    {if (true) return 0;}
      break;
    case 12:
      jj_consume_token(12);
    {if (true) return 1;}
      break;
    default:
      jj_la1[1] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

/* select comm : * or table.attr1, ..., table.attrn */
  static final public NodeList select() throws ParseException {
  NodeList output = null; /* if *, then output is null */
                          /* otherwise, it is a list of "table.attr" */
  NodeData nd;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case 13:
      jj_consume_token(13);
    {if (true) return output;} // if *, just return null value

      break;
    case ID:
      nd = attr();
          output = new NodeList();
          output.insert(nd);
      label_1:
      while (true) {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case 14:
          ;
          break;
        default:
          jj_la1[2] = jj_gen;
          break label_1;
        }
        jj_consume_token(14);
        nd = attr();
          output.insert(nd);
      }
    {if (true) return output;}
      break;
    default:
      jj_la1[3] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

/* from comm : table1, ..., tablen */
  static final public NodeList from() throws ParseException {
  NodeList output = null;
  NodeData nd;
    nd = table();
          output = new NodeList();
          output.insert(nd);
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 14:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_2;
      }
      jj_consume_token(14);
      nd = table();
          output.insert(nd);
    }
    {if (true) return output;}
    throw new Error("Missing return statement in function");
  }

/* where comm : t.attr op val 1, ..., t.attr op val n*/
  static final public NodeList where() throws ParseException {
  NodeList output = null;
  NodeData nd;
    nd = operation();
          output = new NodeList();
          output.insert(nd);
    label_3:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AND:
        ;
        break;
      default:
        jj_la1[5] = jj_gen;
        break label_3;
      }
      jj_consume_token(AND);
      nd = operation();
          output.insert(nd);
    }
    {if (true) return output;}
    throw new Error("Missing return statement in function");
  }

/* Table : "table" */
  static final public NodeData table() throws ParseException {
  NodeData output;
  Token table;
    table = jj_consume_token(ID);
    output = new NodeData();
    output.table_name = table.toString();
    {if (true) return output;}
    throw new Error("Missing return statement in function");
  }

/* attribute : "table.attr" */
  static final public NodeData attr() throws ParseException {
  NodeData output;
  Token table;
  Token attr;
    table = jj_consume_token(ID);
    jj_consume_token(15);
    attr = jj_consume_token(ID);
    output = new NodeData();
    output.table_name = table.toString();
    output.attr_name = attr.toString();
    {if (true) return output;}
    throw new Error("Missing return statement in function");
  }

/* operation : "table.attr op value" */
  static final public NodeData operation() throws ParseException {
  NodeData output;
  Token table;
  Token attr;
  Token operation;
  Token value;
    table = jj_consume_token(ID);
    jj_consume_token(15);
    attr = jj_consume_token(ID);
    operation = jj_consume_token(OP);
    value = jj_consume_token(VALUE);
    output = new NodeData();
    output.table_name = table.toString();
    output.attr_name = attr.toString();
    output.operation = (operation.toString()).charAt(0);
    output.value = Float.parseFloat(value.toString());
    {if (true) return output;}
    throw new Error("Missing return statement in function");
  }

  static private boolean jj_initialized_once = false;
  /** Generated Token Manager. */
  static public SQLparserTokenManager token_source;
  static SimpleCharStream jj_input_stream;
  /** Current token. */
  static public Token token;
  /** Next token. */
  static public Token jj_nt;
  static private int jj_ntk;
  static private int jj_gen;
  static final private int[] jj_la1 = new int[6];
  static private int[] jj_la1_0;
  static {
      jj_la1_init_0();
   }
   private static void jj_la1_init_0() {
      jj_la1_0 = new int[] {0x100,0x1040,0x4000,0x2200,0x4000,0x20,};
   }

  /** Constructor with InputStream. */
  public SQLparser(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public SQLparser(java.io.InputStream stream, String encoding) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new SQLparserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  static public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  static public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  /** Constructor. */
  public SQLparser(java.io.Reader stream) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser. ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new SQLparserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  static public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  /** Constructor with generated Token Manager. */
  public SQLparser(SQLparserTokenManager tm) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser. ");
      System.out.println("       You must either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(SQLparserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  static private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }


/** Get the next Token. */
  static final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

/** Get the specific Token. */
  static final public Token getToken(int index) {
    Token t = token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  static private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  static private java.util.List<int[]> jj_expentries = new java.util.ArrayList<int[]>();
  static private int[] jj_expentry;
  static private int jj_kind = -1;

  /** Generate ParseException. */
  static public ParseException generateParseException() {
    jj_expentries.clear();
    boolean[] la1tokens = new boolean[16];
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 6; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 16; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.add(jj_expentry);
      }
    }
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.get(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  /** Enable tracing. */
  static final public void enable_tracing() {
  }

  /** Disable tracing. */
  static final public void disable_tracing() {
  }

}

//////////////////////////////////////////////////////////////////////////////////////
///////////////////////// TABLE : STORES DB TABLE DATA ///////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

class TableEntry {
        String[] data;

        TableEntry(String[] d) {
          data = d;
        }

        void print() {
          if(data==null) return;
          for (int i=0; i<data.length; i++)
                System.out.print(data[i]+"\u005ct\u005ct");

         System.out.println("");
  }
}

class TableEntryNode {
        TableEntry entry;
        TableEntryNode prev;
        TableEntryNode next;

        TableEntryNode() {
                entry = null;
                prev = null;
                next = null;
        }

        TableEntryNode(String[] d) {
                entry = new TableEntry(d);
                prev = null;
                next = null;
        }

        String[] getData() {
                return entry.data;
        }
        void print() {
          entry.print();
        }

}

// name array, type array
// plus list of table entry arrays
class Table {
        int n_col;
        int n_row;

        TableEntryNode name;
        TableEntryNode type;
        TableEntryNode head;
        TableEntryNode tail;

        Table(String[] na, String[] ty) {
          //init name  type
          name = new TableEntryNode(na);
          type = new TableEntryNode(ty);

          //init head  tail
          head = new TableEntryNode();
          tail = new TableEntryNode();
          head.next = tail;
          tail.prev = head;

          //init num cols  rows
          n_col = na.length;
          n_row = 0;
        }

        Table(Table t) {
          copy(t);
        }

        void insert(String[] d) {
          TableEntryNode temp = new TableEntryNode(d);

          //insert at the tail
          temp.next = tail;
          temp.prev = tail.prev;
          tail.prev.next = temp;
          tail.prev = temp;
          n_row ++;

        }

        void print() {
          name.print();
          type.print();
          TableEntryNode temp = head.next;
          while(true) {
            if(temp.next == null) {
              //System.out.println(""+n_row);
                  //System.out.println("");
                  return;
                }
            temp.print();
            temp = temp.next;
          }
        }

        void copy(Table t) {
          n_col = t.n_col;
          n_row = t.n_row;

          head = new TableEntryNode();
          tail = new TableEntryNode();
          head.next = tail;
          tail.prev = head;

          name = new TableEntryNode(new String[n_col]);
          type = new TableEntryNode(new String[n_col]);
          System.arraycopy(t.name.getData(), 0, name.getData(), 0, n_col);
          System.arraycopy(t.type.getData(), 0, type.getData(), 0, n_col);

          TableEntryNode temp = t.head.next;
          while(true) {
                if(temp.next == null) return;
            String[] stemp = new String[n_col];
                System.arraycopy(temp.entry.data, 0, stemp, 0, n_col);
                insert(stemp);
                temp = temp.next;
         }
  }

}


//////////////////////////////////////////////////////////////////////////////////////
//////////////////// TABLE LIST : ORGANIZES TABLES (w/ NAMES) ////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

class TableNode {
  Table table;
  String name;
  TableNode prev;
  TableNode next;

  TableNode() {
    prev = null;
    next = null;
    table = null;
    name = "";
  }

  TableNode(Table t, String n) {
        prev = null;
        next = null;
        table = t;
        name = n;
  }


}
class TableList {
  TableNode head;
  TableNode tail;

  TableList() {
        head = new TableNode();
        tail = new TableNode();
        head.next = tail;
        tail.prev = head;
  }

  void insert(Table t, String name) {
          TableNode temp = new TableNode(t, name);

          //insert at the tail
          temp.next = tail;
          temp.prev = tail.prev;
          tail.prev.next = temp;
          tail.prev = temp;

        }

  Table search(String name) {
          TableNode temp = head.next;
          while(true) {
            if(temp.next == null) return null; // reach at the tail, then no corresponding table 
            if(name.equals(temp.name)) return temp.table;
            temp = temp.next; // progress one step
        }
  }

}




//////////////////////////////////////////////////////////////////////////////////////
///////////////////// NODE LIST :  STORES COMMENT PARSING RESULT /////////////////////
//////////////////////////////////////////////////////////////////////////////////////

/* Node_data stores 'table.attr' or 'table.attr op value' */
class NodeData {
  String table_name;
  String attr_name;
  char operation; /* =, <, > only */
  float value;

  public NodeData() {
  }

  public NodeData(String tn, String an, char op, float val) {
    table_name = tn;
    attr_name = an;
    operation = op;
    value = val;
  }

}

/* Node element for Node List */
/* It encapsulates the Node Data */
class Node {
  NodeData node;
  Node prev;
  Node next;

  public Node() {
        node = new NodeData();
        prev = null;
        next = null;
  }

  public Node(NodeData nd) {
    node = nd;
    prev = null;
    next = null;
  }

  public Node(NodeData nd, Node p, Node n) {
    node = nd;
    prev = p;
    next = n;
  }
}

/* Node List stores multiple node_data in  a list */
class NodeList {
        Node head;
        Node tail;

        public NodeList() {
          head = new Node();
          tail = new Node();
          head.next = tail;
          tail.prev = head;
        }

        public void insert(NodeData nd) {
          Node n_temp = new Node(nd);
          n_temp.prev = tail.prev;
          n_temp.next = tail;
          tail.prev.next = n_temp;
          tail.prev = n_temp;
        }

        public void print() {
                System.out.print("Print Node List : ");
                Node temp = head.next;
                while(temp.next != null) {
                    NodeData nd = temp.node;
                        System.out.print("("+ nd.table_name + ","+ nd.attr_name + "," + nd.operation + "," + Float.toString(nd.value)+ ") ");
                        temp = temp.next;
                }
                System.out.println("");
        }
}
