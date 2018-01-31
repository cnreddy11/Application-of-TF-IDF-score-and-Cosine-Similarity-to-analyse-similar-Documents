/*
   Author : Nikhila Chireddy
   Date : 03-23-2017
*/

import java.util.ArrayList;
import java.util.List;

public class Ngram {
	public String data;
	public String date;
	public String author;
	List<String> unigram = new ArrayList<String>();
	List<String> bigram = new ArrayList<String>();
	public Ngram(String S)
	{
		String[] line = S.toString().split("<===>");
		author = line[0];
		date = line[1];
		data= line[2];
		author = author.substring(author.lastIndexOf(" ")+1);
		author= author.replaceAll("[^A-Za-z ]", "");
		unigramGenerator();
	}
	public void unigramGenerator()
	{
		data = data.replaceAll("[^A-Z0-9a-z]", " ").trim();
		data = data.toLowerCase();
		String[] divide = data.toString().split(" ");
		for(String s: divide)
		{
			if(!s.isEmpty())
				unigram.add(s.toString());
		}
	}
}
