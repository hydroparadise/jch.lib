package jch.lib.analytics.text.book.bible;

import java.util.ArrayList;

import jch.lib.analytics.text.book.BookChapter;

public class BibleChapter extends BookChapter {
	int chapterNumber;
	public int getChapterNumber() {
		return chapterNumber;
	}
	public void setChapterNumber(int chapterNumber) {
		this.chapterNumber = chapterNumber;
	}
	
	public ArrayList<BibleVerse> Verses;
	
	public BibleChapter() {
		// TODO Auto-generated constructor stub
		Verses = new ArrayList<BibleVerse>();
	}
	
	public BibleChapter(int chapterNumber) {
		// TODO Auto-generated constructor stub
		Verses = new ArrayList<BibleVerse>();
		this.chapterNumber = chapterNumber;
	}

}
