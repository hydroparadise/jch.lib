package jch.lib.analytics.text.book;

import java.util.ArrayList;

import jch.lib.list.ChunkLink;

public class Book extends BookValue{
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
	
	public ArrayList<BookChapter> Chapters;
	
	public Book() {
		Chapters = new ArrayList<BookChapter>();
	}
	
	

}
