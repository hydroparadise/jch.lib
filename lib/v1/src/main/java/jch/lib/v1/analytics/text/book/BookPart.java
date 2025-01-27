package jch.lib.analytics.text.book;



import jch.lib.list.ChunkLink;

public class BookPart extends BookValue {
	public ChunkLink getStartContentRef() {
		return startContentRef;
	}

	public void setStartContentRef(ChunkLink startContentRef) {
		this.startContentRef = startContentRef;
	}

	public ChunkLink getEndContentRef() {
		return endContentRef;
	}

	public void setEndContentRef(ChunkLink endContentRef) {
		this.endContentRef = endContentRef;
	}

	ChunkLink startContentRef;
	ChunkLink endContentRef;
	
	public BookPart() {
		// TODO Auto-generated constructor stub
	}

}
