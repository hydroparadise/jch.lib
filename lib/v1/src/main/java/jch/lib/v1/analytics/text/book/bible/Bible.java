package jch.lib.analytics.text.book.bible;

import java.util.ArrayList;

import jch.lib.analytics.text.book.BookValue;

public class Bible extends BookValue {
	public ArrayList<Testament> Testaments;
	
	
	Testament currentTestament;
	BibleBook currentBook;
	BibleChapter currentChapter;
	BibleVerse currentVerse;
	
	public int getIdxTestament() {
		return idxTestament;
	}

	public void setIdxTestament(int idxTestament) {
		this.idxTestament = idxTestament;
	}

	public int getIdxBook() {
		return idxBook;
	}

	public void setIdxBook(int idxBook) {
		this.idxBook = idxBook;
	}

	public int getIdxChapter() {
		return idxChapter;
	}

	public void setIdxChapter(int idxChapter) {
		this.idxChapter = idxChapter;
	}

	public int getIdxVerse() {
		return idxVerse;
	}

	public void setIdxVerse(int idxVerse) {
		this.idxVerse = idxVerse;
	}
	int idxTestament;
	int idxBook;
	int idxChapter;
	int idxVerse;
	

	public Bible() {
		Testaments = new ArrayList<Testament>();
		idxTestament = 0;
		idxBook = 0;
		idxChapter = 0;
		idxVerse = 0;
	}

	public BibleVerse getCurrentVerse() {
		return this.Testaments.get(idxTestament)
				   .Books.get(idxBook)
				   .Chapters.get(idxChapter)
				   .Verses.get(idxVerse);
	}
	
	public Testament getCurrentTestament() {
		return	this.Testaments.get(idxTestament);
	}
	
	public BibleBook getCurrentBook() {
		return	this.Testaments.get(idxTestament)
				   .Books.get(idxBook);
	}
	
	public BibleChapter getCurrentChapter() {
		return	this.Testaments.get(idxTestament)
				   .Books.get(idxBook)
				   .Chapters.get(idxChapter);
	}
	
	public boolean moveFirstVerse() {
		idxTestament = 0;
		idxBook = 0;
		idxChapter = 0;
		idxVerse = 0;
		
		return true;
	}
	
	public boolean moveNextVerse() {
		if(idxVerse < this.Testaments.get(idxTestament)
				   .Books.get(idxBook)
				   .Chapters.get(idxChapter)
				   .Verses.size() - 1){
			idxVerse++;
			return true;
		}
		else 
		if (idxChapter < this.Testaments.get(idxTestament)
				             .Books.get(idxBook)
				             .Chapters.size() - 1){
			idxVerse = 0;
			idxChapter ++;
			return true;
		}
		else 
		if(idxBook < this.Testaments.get(idxTestament)
				         .Books.size() -1) {
			idxVerse = 0;
			idxChapter = 0;
			idxBook++;
			return true;
		}
		else
		if(idxTestament < this.Testaments.size() - 1) {
			idxVerse = 0;
			idxChapter = 0;
			idxBook = 0;
			idxTestament++;
			return true;
		}
		else return false;
	}
	public boolean movePreviousVerse() {
		return false;
	}
	
}
