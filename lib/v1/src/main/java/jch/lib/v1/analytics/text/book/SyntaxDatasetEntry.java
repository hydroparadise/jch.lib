package jch.lib.analytics.text.book;

//for receiving google analytics object properties
public interface SyntaxDatasetEntry {
	public String getText();
	public void setText(String text);
	public int getBeginOffset();
	public void setBeginOffset(int beginOffset) ;
	public String getLemma();
	public void setLemma(String lemma);
	public String getPartOfSpeechTag();
	public void setPartOfSpeechTag(String partOfSpeechTag);
	public String getAspect();
	public void setAspect(String aspect);
	public String getCasing();
	public void setCasing(String casing);
	public String getForm();
	public void setForm(String form) ;
	public String getGender();
	public void setGender(String gender);
	public String getMood();
	public void setMood(String mood);
	public String getNumber();
	public void setNumber(String number);
	public String getPerson();
	public void setPerson(String person);
	public String getProper();
	public void setProper(String proper);
	public String getReciprocity() ;
	public void setReciprocity(String reciprocity);
	public String getTense();
	public void setTense(String tense) ;
	public String getVoice();
	public void setVoice(String voice);
	public int getDependencyEdgeHeadToken();
	public void setDependencyEdgeHeadToken(int dependencyEdgeHeadToken);
	public String getDependencyEdgeLabel() ;
	public void setDependencyEdgeLabel(String dependencyEdgeLabel);
	public static String getHeaderString() {return null;}
	//public static void setHeaderString(String headerString) {}
	public String getStringRecord();
	//public boolean setStringRecord(String record);
	public String getPunctuation();
	public String getMsrPDP();
}
