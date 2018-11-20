
public class Join {
	int jmode = 0; // join mode, 0 for bnj, 1 for smj, 2 for hj
	
	int bsize; // B : number of buffers
	int rnum; // number of records per page
	boolean timing;
	
	int strLen;
	int tupleLenIn;
	int tupleLenOut;
	
	int colIn;
	int typeIn;
	int colIndexIn;
	
	int colOut;
	int typeOut;
	int colIndexOut;
	
	char op;
	
	// buffers
	char[][] inbuffer;
	char[][] buffer;
	char[][] outbuffer;
	
	Join(){
		bsize = 4;
		rnum = 2;		
		timing = false;
	}
	
	/* Setters */
	void setBsize(int _bsize){
		if(_bsize<3) {
			System.out.println("Error : Bsize should be at least 3");
			return;
		}
		bsize = _bsize;
	}
	
	void setRnum(int _rnum) {
		if(_rnum <1) {
			System.out.println("Error : Rnum should be at least 1");
			return;
		}
		rnum = _rnum;
	}
	
	void setTiming(boolean _timing) {
		timing = _timing;
	}
	
	
	void init(int tlout, int tlin, int sl, int colin, int typein, int colout, int typeout, char _op) {
		/* initiate the buffer size */
		/* # buffer = bsize, records per buffer = rnum, record length = tupleLen */
		tupleLenIn = tlin;
		tupleLenOut = tlout;
		
		strLen = sl;
		
		colIn = colin;
		typeIn = typein;
		colIndexIn = colIn*strLen;
		
		colOut = colout;
		typeOut = typeout;
		colIndexOut = colOut*strLen;
		
		/* for convenience, let's assume that the absolute size of buffers can be different,
		 * depending on the tuple size they contain */
		
		// inbuffer for the outer
		inbuffer = new char[rnum][tupleLenIn];
		// buffer for the inner
		buffer = new char[rnum*(bsize-2)][tupleLenOut];
		// outbuffer
		outbuffer = new char[rnum][tupleLenIn+tupleLenOut];
		
		op = _op;
		
	}
	
	Table run(Table inner, Table outer, int colin, int typein, int colout, int typeout, char op) {
		init(outer.tupleLen, inner.tupleLen, inner.strLen, colin, typein, colout, typeout, op);
		// check for join-ability
		// must have same types
		if(typeIn!=typeOut) {
			System.out.println("Error : Joining columns have different types");
			return null;
		}
		// for sort merge join and hash join, operation must be '='
		if(op!='=' && jmode!=0) {
			System.out.println("Error : Sort merge join and hash join only support equality operation");
			return null;
		}
				
		return null;
	}
	
	Table blockNestedJoin() {
		
	}
	
}
