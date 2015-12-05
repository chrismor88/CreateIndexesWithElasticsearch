package bean;

import java.util.LinkedList;
import java.util.List;

public class WikipediaTable {

	private String idTable;
	private String wikid;
	private int nrows;
	private int ncols;
	private int nrows_with_multiple_mentions;
	List<String> rows;
	
	
	public WikipediaTable() {
		this.rows = new LinkedList<String>();
	}


	public String getIdTable() {
		return idTable;
	}


	public String getWikid() {
		return wikid;
	}


	public int getNrows() {
		return nrows;
	}


	public int getNcols() {
		return ncols;
	}


	public List<String> getRows() {
		return rows;
	}


	public void setIdTable(String idTable) {
		this.idTable = idTable;
	}


	public void setWikid(String wikid) {
		this.wikid = wikid;
	}


	public void setNrows(int nrows) {
		this.nrows = nrows;
	}


	public void setNcols(int ncols) {
		this.ncols = ncols;
	}


	public void setRows(List<String> rows) {
		this.rows = rows;
	}
	
	public void addRow(String row){
		this.rows.add(row);
	}
	
	public List<String> firstFourRows(){
		int maxShowRows = 4;
		if(rows.size() < maxShowRows)
			maxShowRows = rows.size();
		
		return rows.subList(0, maxShowRows-1);
		
	}
	
	public void setNrowsWithMultipleMentions(int k) {
		this.nrows_with_multiple_mentions = k;
	}
	
	public int getNrows_with_multiple_mentions() {
		return nrows_with_multiple_mentions;
	}
	
	@Override
	public String toString() {
		return this.idTable+"\t"+this.wikid+"\t"+this.ncols+"\t"+this.nrows+"\t"+this.getRows();
	}
	
}
