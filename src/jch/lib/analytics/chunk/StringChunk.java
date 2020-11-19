package jch.lib.analytics.chunk;

public class StringChunk {
	private String value = "";
	
	public StringChunk(String newValue) {
		if(newValue != null) {
			this.value = newValue;
		}
	}
	
	public String getValue() {
		return value;
	}
	
	public boolean setValue(String newValue) {
		if(this.value != "" || newValue != null) {
			this.value = newValue;
			return true;
		}
		else return false;
	}


}
