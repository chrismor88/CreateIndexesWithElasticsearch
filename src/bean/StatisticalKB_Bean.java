package bean;

import java.util.Map;

public class StatisticalKB_Bean {
	
	private Map<String,Integer> relationTypeToOccurrence;
	private int mid1_found_counter;
	private int mid2_found_counter;
	
	public StatisticalKB_Bean() {
		
	}
	
	public void setMid1_found_counter(int mid1_foud_counter) {
		this.mid1_found_counter = mid1_foud_counter;
	}
	
	public void setMid2_found_counter(int mid2_found_counter) {
		this.mid2_found_counter = mid2_found_counter;
	}
	
	public void setRelationTypeToOccurrence(Map<String, Integer> relationTypeToOccurrence) {
		this.relationTypeToOccurrence = relationTypeToOccurrence;
	}
	
	public int getMid1_found_counter() {
		return mid1_found_counter;
	}
	
	public int getMid2_found_counter() {
		return mid2_found_counter;
	}
	
	public Map<String, Integer> getRelationTypeToOccurrence() {
		return relationTypeToOccurrence;
	}

}
