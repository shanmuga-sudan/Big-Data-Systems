package org.midtermcode.geneticalgorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class Chomosone implements WritableComparable<Chomosone> {
	
	static char[] ltable = {'0','1','2','3','4','5','6','7','8','9','+','-','*','/'};
	static int chromoLen = 5;
	static double crossRate = .7;
	static double mutRate = .001;
	static Random rand = new Random();
	
	// Genetic Algorithm Node
			// The chromo
			private StringBuffer chromo		  = new StringBuffer(chromoLen * 4);
			private StringBuffer decodeChromo = new StringBuffer(chromoLen * 4);
			private double score;
			private int total;
			
			// Constructor that generates a random
			public void getMyChomosone(int target) {
				
				// Create the full buffer
				for(int y=0;y<chromoLen;y++) {
					// What's the current length
					int pos = chromo.length();
					
					// Generate a random binary integer
					String binString = Integer.toBinaryString(rand.nextInt(ltable.length));
					int fillLen = 4 - binString.length();
					
					// Fill to 4
					for (int x=0;x<fillLen;x++) chromo.append('0');
					
					// Append the chromo
					chromo.append(binString);
//					System.out.println(chromo+"is chromo");
					
				}
				
				// Score the new cromo
//				System.out.println("Checking in to score");
				scoreChromo(target);
			}
						
			public Chomosone(StringBuffer chromo)
			{ 
				this.chromo = chromo;
			}
			public Chomosone()
			{ 
				
			}
			
	

	public static Chomosone selectMember(ArrayList l) { 

		// Get the total fitness
		double tot=0.0;
		for (int x=l.size()-1;x>=0;x--) {
			double score = ((Chomosone)l.get(x)).getScore();
			tot+=score;
		}
		double slice = tot*rand.nextDouble();
		
		// Loop to find the node
		double ttot=0.0;
		for (int x=l.size()-1;x>=0;x--) {
			Chomosone node = (Chomosone)l.get(x);
			ttot+=node.getScore();
			if (ttot>=slice) { l.remove(x); return node; }
		}
		if(!(l.size()<=0)){
			return (Chomosone)l.remove(l.size()-1);
		}
		else{
			return null;
		}
		
	}
		// Decode the string
		public final String decodeChromo() {	

			// Create a buffer
			getDecodeChromo().setLength(0);
			
			// Loop throught the chromo
			for (int x=0;x<getChromo().length();x+=4) {
				// Get the
				int idx = Integer.parseInt(getChromo().toString().substring(x,x+4), 2);
				if (idx<ltable.length) getDecodeChromo().append(ltable[idx]);
			}
			
			// Return the string
//			System.out.println("The decodedvalue is : "+getDecodeChromo().toString());
			return getDecodeChromo().toString();
		}
		
		// Scores this chromo
		public final double scoreChromo(int target) {
//			
			total = addUp();
//			System.out.println("total is "+total);
			if (total == target) score = 0;
			score = Math.abs((double)1 / (target - total));
			
//			System.out.println(score+" this is score");
			return score;
		}
		
		// Crossover bits
		public final void crossOver(Chomosone other) {

			// Should we cross over?
			if (rand.nextDouble() > crossRate) return;
			
			// Generate a random position
			System.out.println(getChromo().toString().length()+" is the chromo length");
			int pos =rand.nextInt(getChromo().length()) ;
			
			
			// Swap all chars after that position
			for (int x=pos;x<getChromo().length();x++) {
				// Get our character
				char tmp = getChromo().charAt(x);
				
				// Swap the chars
				getChromo().setCharAt(x, other.getChromo().charAt(x));
				other.getChromo().setCharAt(x, tmp);
//				System.out.println("This is cross-over speaking");
			}
		}
			
		// Mutation
		public final void mutate() {
			for (int x=0;x<getChromo().length();x++) {
					getChromo().setCharAt(x, (getChromo().charAt(x)=='0' ? '1' : '0'));
			}
//			System.out.println("This is mutate");
		}
			
		
				
		// Add up the contents of the decoded chromo
		public final int addUp() { 
		
			// Decode our chromo
			String decodedString = decodeChromo();
//			System.out.println("length is "+decodedString.length());
			// Total
			int tot = 0;
			
			// Find the first number
			int ptr = 0;
			while (ptr<decodedString.length()) { 
				char ch = decodedString.charAt(ptr);
				if (Character.isDigit(ch)) {
					tot=ch-'0';
					ptr++;
					break;
				} else {
					ptr++;
				}
			}
			
			// If no numbers found, return
			if (ptr==decodedString.length()) return 0;
			
			// Loop processing the rest
			boolean num = false;
			char oper=' ';
			while (ptr<decodedString.length()) {
				// Get the character
				char ch = decodedString.charAt(ptr);
				
				// Is it what we expect, if not - skip
				if (num && !Character.isDigit(ch)) {ptr++;continue;}
				if (!num && Character.isDigit(ch)) {ptr++;continue;}
			
				// Is it a number
				if (num) { 
					switch (oper) {
						case '+' : { tot+=(ch-'0'); break; }
						case '-' : { tot-=(ch-'0'); break; }
						case '*' : { tot*=(ch-'0'); break; }
						case '/' : { if (ch!='0') tot/=(ch-'0'); break; }
					}
				} else {
					oper = ch;
				}			
				
				// Go to next character
				ptr++;
				num=!num;
			}
//			System.out.println("the totoal is "+tot);
			return tot;
		}

		public final boolean isValid() { 
		
			// Decode our chromo
			String decodedString = decodeChromo();
			
			boolean num = true;
			for (int x=0;x<decodedString.length();x++) {
				char ch = decodedString.charAt(x);

				// Did we follow the num-oper-num-oper-num patter
				if (num == !Character.isDigit(ch)) return false;
				
				// Don't allow divide by zero
				if (x>0 && ch=='0' && decodedString.charAt(x-1)=='/') return false;
				
				num = !num;
			}
			
			// Can't end in an operator
			if (!Character.isDigit(decodedString.charAt(decodedString.length()-1))) return false;
			
			return true;
		}

		public double getScore() {
			return score;
		}

		public void setScore(double score) {
			this.score = score;
		}		

		public int getTotal() {
			return total;
		}

		public void setTotal(int total) {
			this.total = total;
		}
		

		public StringBuffer getChromo() {
			return chromo;
		}

		public void setChromo(StringBuffer chromo) {
			this.chromo = chromo;
		}

		public StringBuffer getDecodeChromo() {
			return decodeChromo;
		}

		public void setDecodeChromo(StringBuffer decodeChromo) {
			this.decodeChromo = decodeChromo;
		}

//		@Override
//		public String toString(){
////			return decodeChromo().toString();
//			return ((getChromo().toString()));
//		}
		@Override
		public void readFields(DataInput di) throws IOException{
			score=di.readDouble();
			total=di.readInt();
			String temp="";
			String tempValue=WritableUtils.readString(di);
			chromo=chromo.append(temp+tempValue);
			String tempValue1=WritableUtils.readString(di);
			decodeChromo=decodeChromo.append(temp+tempValue1);
			
		}
		
		@Override
		public void write(DataOutput d) throws IOException{
			d.writeInt(total);
			d.writeDouble(score);
			WritableUtils.writeString(d,chromo.toString());
			WritableUtils.writeString(d,decodeChromo.toString());
		}

		@Override
		public int compareTo(Chomosone ct) {
			// TODO Auto-generated method stub
			double thisScore=this.score;
			double thatScore=ct.score;
			 if(Double.compare(thatScore,thisScore)<0)
				 return 1;
			 else if(Double.compare(thatScore,thisScore)==0)
				 return 0;
			 else
				 return -1;
			 
		}
		
//		@Override
//		 public int hashCode() {
//	         final int prime = 31;
//	         int result = 1;
//	         result = prime * result + counter;
//	         result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
//	         return result;
//	       }
		
		

	
}
