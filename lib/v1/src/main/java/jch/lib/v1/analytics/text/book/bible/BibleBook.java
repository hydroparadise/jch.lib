package jch.lib.analytics.text.book.bible;

import java.util.ArrayList;

import jch.lib.analytics.text.book.BookValue;
import jch.lib.list.ChunkLink;

public class BibleBook extends BookValue {
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
	
	public ArrayList<BibleChapter> Chapters;
	
	public BibleBook() {
		super();
		Chapters = new ArrayList<BibleChapter>();
		
	}
}
