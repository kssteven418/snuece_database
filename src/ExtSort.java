import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class ExtSort {
	
	/* modes : modified by setter functions only */
	int bsize; // B : number of buffers
	int rnum; // number of records per page
	boolean timing;
	
	/* belows are initiate by init function */
	
	/* tuple and string length */
	int tupleLen;
	int strLen;
	
	//column number to sort by and its type
	int col;
	int type;
	int colIndex;
	
	//output buffer position(index of the last page)
	int outbufPos;
	
	//length of current run : B -> B(B-1) -> B(B-1)^2 -> ...
	int runLen;
	
	// buffer
	char[] buffer;
	
	ExtSort(){
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
	
	
	/* DEBUGGING MODULES */
	// automatic debugging function
	void runTest(Table input) {
		for(int b=3; b<10; b++) {
			for(int r=2; r<10; r++) {
				setBsize(b);
				setRnum(r);
				init(input.tupleLen, input.strLen, 1, 0);
				
				boolean succ = true;
				Table newinput = new Table(input.tname, input.ncol, input.names, input.types);
				for(int i=0; i<input.data.size(); i++) {
					newinput.insert(input.data.get(i));
					Table output = run(newinput, false);
					
					boolean tempSucc = checkOutput(output);
					
					if(!tempSucc) {
						succ = false;
						System.out.println("Wrong for B: "+b+", R: "+r+", i: "+i);
					}
				}
				if(succ) {
					System.out.println("Success for B: "+b+", R: "+r);
				}
			}
			
		}
		
		
	}
	
	boolean checkOutput(Table output) {
		for(int i=0; i<output.data.size()-1; i++) {
			String temp = CharStr.getString(output.data.get(i), colIndex);
			String temp2 = CharStr.getString(output.data.get(i+1), colIndex);
			if(type==0) {
				if(Integer.parseInt(temp)>Integer.parseInt(temp2)) 
					return false;
			}
			else {
				if(temp.compareTo(temp2)>0)
					return false;
			}
		}
		return true;
	}
	
	/*********************** INIT and RUN FUNCTIONS **************************/
	
	// initiation
	// tl:tupleLen, sl:strLen(expecting 21)
	// col : column to sort by, type : type of col
	void init(int tl, int sl, int _col, int _type) {
		/* initiate the buffer size */
		/* # buffer = bsize, records per buffer = rnum, record length = tupleLen */
		tupleLen = tl;
		strLen = sl;
		
		col = _col;
		type = _type;
		colIndex = col*strLen;
		
		buffer = new char[bsize*rnum*tupleLen];
		// starting point of the output buffer (last page starting index)
		outbufPos = bsize*rnum - rnum; 
		
		runLen = 0;
	}
		
	
	Table run(Table input, int _col, int _type, boolean print) {
		init(input.tupleLen, input.strLen, _col, _type);
		Table temp = run(input, print);
		//temp.print();
		
		if(print) {
			boolean result = checkOutput(temp);
			System.out.println("RESULT : "+result);
			System.out.println();
		}
		
		return temp;
	}
	
	Table run(Table input, boolean print) {
		if(input.data.size()==0) return null;
		
		Table temp = run(input, 0);
		int i = 0;
		
		if(print) {
			System.out.println("PASS "+i);
			temp.print();
		}
		
		//write into file
		writeFile(temp, i);
		
		i++;
		
		//pass until run size becomes bigger than input size
		while(true) {
			if(input.data.size()<=runLen*rnum) {
				break;
			}
			temp = run(temp, i);
			if(print) {
				System.out.println("PASS "+i);
				temp.print();
			}

			//write into file
			writeFile(temp, i);
			
			i++;
		}
		
		return temp;
	}
	
	Table run(Table input, int pass) {
		Table output = new Table(input.tname, input.ncol, input.names, input.types);
		
		//internal sort
		if(pass==0) {
			int inputStartPos = 0;
			while(true) {
				int start = 0;
				for(; start<bsize*rnum; start++) {
					if(inputStartPos+start >= input.data.size()) break; // index out of bound
					char[] temp = input.data.get(inputStartPos+start); // read from input
					System.arraycopy(temp, 0, buffer, tupleLen*start, tupleLen); // copy at the buffer
				}
				internalSort(0, start-1); // sort tuples in the buffer
				dumpBuffer(output, 0, start-1); // dump into the output table
				
				if(inputStartPos+start >= input.data.size()) break;// index out of bound
				
				inputStartPos += bsize*rnum;			
			}
			
			runLen = bsize;
			
		}
		
		//external merge sort
		else {
			//outer loop
			int inputStartPos = 0;
			boolean finish = false;
			
			char[] nullString = new char[tupleLen];
			for(int k=0; k<tupleLen; k++) nullString[k] = '\0'; // make null string
			
			while(!finish) {
				int[] pointerBuffer = new int[bsize-1]; 
				int[] pointerTable = new int[bsize-1];
				int[] intlist = new int[bsize-1]; //pointing value for type 0
				String[] strlist = new String[bsize-1]; // pointing value for type 1
				boolean[] availlist = new boolean[bsize-1];
				
				
				/******************* Buffer Initiation Phase *******************/
				
				for(int i=0; i<bsize-1; i++) { 
					pointerBuffer[i] = rnum*i; //next tuple to output per each page
					pointerTable[i] = inputStartPos + rnum*runLen*i; // next input table entry to read per each page
				}
				
				/* read one page per run, and fill in the buffer */
				for(int i=0; i<bsize-1; i++) { // for each run
					for(int j=0; j<rnum; j++) { // copy rnum tuples of one page
						
						if(pointerTable[i]>=input.data.size()){
							finish=true; // break the while loop
							
							System.arraycopy(nullString, 0,
									buffer, tupleLen*(pointerBuffer[i]+j), tupleLen);
						}

						else{
							System.arraycopy(input.data.get(pointerTable[i]), 0,
									buffer, tupleLen*(pointerBuffer[i]+j), tupleLen);
						}
						pointerTable[i]++;
					}
				}
				
				// initiate the int/string lists
				for(int i=0; i<bsize-1; i++) {
					updateList(intlist, strlist, availlist, pointerBuffer, i);
				}
				
				
				/********************** Merge Phase **********************/

				int outbufPointer = outbufPos; // where to write output tuple
				
				while(true) {
										
					/* Ending Phase */
					//if all invalidate. than break the loop
					boolean breakCond = true;
					for(int i=0; i<bsize-1; i++) {
						if(availlist[i]) {
							breakCond = false; 
							break;
						}
					}
					if(breakCond) {
						//dump remaining outputBuf
						dumpBuffer(output, outbufPos, outbufPointer-1);
						break;
					}
					
					// get minimum index
					int minIndex = minIndex(intlist, strlist, availlist);
					
					
					/* output phase */					
					// write outputTuple to the output buffer
					System.arraycopy(buffer, tupleLen*pointerBuffer[minIndex],
							buffer, tupleLen*outbufPointer, tupleLen);
					
					// if full, dump the output buffer
					if(outbufPointer == outbufPos + rnum - 1) {
						//System.out.println(buffer);
						dumpBuffer(output, outbufPos, outbufPointer);
						outbufPointer = outbufPos;
					}
					// else, simply proceed the outbufPointer
					else {
						outbufPointer += 1;
					}
					
					
					/* input phase */
					//proceed the buffer pointer
					pointerBuffer[minIndex] += 1;
					
					//if pointer is out of bound
					if(pointerBuffer[minIndex]>=(minIndex+1)*rnum){
						// pointer back to the first index
						pointerBuffer[minIndex] = rnum*minIndex;
						
						//read from the input table
						//if pointer is out of bound, then copy nullString
						if(pointerTable[minIndex] >= inputStartPos + rnum*runLen*(minIndex+1)) {
							for(int j=0; j<rnum; j++) { 
									System.arraycopy(nullString, 0,
											buffer, tupleLen*(pointerBuffer[minIndex]+j), tupleLen);
							}
						}
						//if pointer is not out of bound
						else {
							for(int j=0; j<rnum; j++) { // copy rnum tuples of one page
								
								if(pointerTable[minIndex]>=input.data.size()){
									System.arraycopy(nullString, 0,
											buffer, tupleLen*(pointerBuffer[minIndex]+j), tupleLen);
								}

								else{
									System.arraycopy(input.data.get(pointerTable[minIndex]), 0,
											buffer, tupleLen*(pointerBuffer[minIndex]+j), tupleLen);
								}
								pointerTable[minIndex]++;
							}
						}
					}
					
					// then, update the int/string lists
					updateList(intlist, strlist, availlist, pointerBuffer, minIndex);
					
				}
				
				inputStartPos += rnum*runLen*(bsize-1); // next set of runs
			}
			
			runLen *= (bsize-1);
		}
		
		//System.out.println("PASS "+pass);
		//output.print();
		return output;
	}
	
	
	/************************* HELPER FUNCTIONS ***************************/
	
	// helper function for the 0th pass
	// sort buffer from (startPos)th tuple to (endPos)th tuple
	void internalSort(int startPos, int endPos) {
		//use bubble sort
		for(int i=startPos; i<endPos; i++) {
			for(int j=i+1; j<=endPos; j++) {
				String x = CharStr.getString(buffer, i*tupleLen+colIndex);
				String y = CharStr.getString(buffer, j*tupleLen+colIndex);
				try {
					if((type==1 && x.compareTo(y)>0) || (type==0 && Integer.parseInt(x) > Integer.parseInt(y))) {
						swap(i, j);					}
				} catch(Exception e) {
					System.out.println("Error : Type casting error.");
				}
			}
		}
		
		//should adjust quicksort..??
		
	}
	
	// helper function for interalSort function
	// swap (x_pos)th tuple with (y_pos)th tuple
	void swap(int x_pos, int y_pos) {
		char[] temp = new char[tupleLen];
		System.arraycopy(buffer, x_pos*tupleLen, temp, 0, tupleLen); // x->temp
		System.arraycopy(buffer, y_pos*tupleLen, buffer, x_pos*tupleLen, tupleLen); // y->x
		System.arraycopy(temp, 0, buffer, y_pos*tupleLen, tupleLen); // temp->y
	}
	
	
	// helper function 
	// get the index of the available minimum value out of int/string list
	int minIndex(int[] intlist, String[] strlist, boolean[] availlist) {
		int index = -1;
		int minInt = 0;
		String minString = "";
		
		for(int i=0; i<availlist.length; i++) {
			if(availlist[i]) {
				if(index<0) { // first time
					index = i;
					if(type == 1) minString = strlist[i];
					else minInt = intlist[i];
				}
				else {
					if(type==0 && minInt>intlist[i]) {
						minInt = intlist[i];
						index = i;
					}
					else if(type==1 && minString.compareTo(strlist[i])>0) {
						minString = strlist[i];
						index = i;
					}
				}
			}
		}
		
		return index;
	}
	
	//helper function
	//update the int/string lists
	void updateList(int[] intlist, String[] strlist, boolean[] availlist, int[] pointerBuffer, int i) {
		String temp = CharStr.getString(buffer, pointerBuffer[i]*tupleLen+colIndex);
		if(temp.length()==0) {
			availlist[i] = false;
		}
		else {
			availlist[i] = true;
			if(type == 1) 
				strlist[i] = temp;
			else {
				try {
					intlist[i] = Integer.parseInt(temp);
				} catch(Exception e) {
					System.out.println("Error : Type casting error.");
				}
			}
		}
	}
	
	// dump (startPos)th ~ (endPos)th tuples in the buffer to the output table
	void dumpBuffer(Table table, int startPos, int endPos) {
		for(int i=startPos; i<=endPos; i++) {
			char[] temp = new char[tupleLen];
			System.arraycopy(buffer, i*tupleLen, temp, 0, tupleLen);
			table.insert(temp);
		}
	}
	
	
	
	/************************* WRITE FUNCTIONS ***************************/
	
	void writeFile(Table table, int pass) {
		String fn = "src/runs/"+table.tname+"_"+pass+"_";
		int r = 0;
		int i = 0;
		boolean finish = true;
		
		while(finish) {
			File file = new File(fn+r+".txt");	
			try {
				FileWriter fw = new FileWriter(file);
				for(int j=0; j<runLen*rnum; j++) {
					if(i>=table.data.size()) {
						finish = false;
						break;
					}
					String tuple = "";
					for(int k=0; k<table.ncol; k++) {
						tuple = tuple + CharStr.getString(table.data.get(i), table.strLen*k) + "\t";
					}
					tuple = tuple + "\n";
					
					//System.out.print(tuple);
					fw.write(tuple);
					i++;
				}
				fw.close();
				//System.out.println("----");
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			r++;
		}
		//System.out.println(r);
	}
	
}






// static functions to handle String and characters
class CharStr{
	
	static void copyString(char[] dest, int destInd, String src, int len) {
		int temp = Math.min(src.length(), len);
		src.getChars(0, temp, dest, destInd);
		dest[destInd+temp] = '\0';
	}
	
	static String getString(char[] src, int srcInd) {
		int i = srcInd;
		for(; i<src.length; i++) {
			if(src[i]=='\0') break;
		}
		int len = i-srcInd;
		return new String(src, srcInd, len);
	}
	
}