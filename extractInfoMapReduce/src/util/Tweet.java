package util;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;

public class Tweet {
	
	private int userId;
	private String userName;
	private double latitude;
	private double longitude;
	private String text;
	private int followers;
	private Calendar date;
	
	
	public Calendar getDate() {
		return date;
	}

	public void setDate(Calendar date) {
		this.date = date;
	}

	public Tweet(){}
	
	public Tweet(int userId, String userName, double latitude, double longitude,
			String text,Calendar date) {
		super();
		this.userId = userId;
		this.userName = userName;
		this.latitude = latitude;
		this.longitude = longitude;
		this.text = text;
		this.date = date;
		//this.followers = followers;
	}
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(int latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(int longitude) {
		this.longitude = longitude;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public int getFollowers() {
		return followers;
	}
	public void setFollowers(int followers) {
		this.followers = followers;
	}
	
//	public String toString(){
//		return this.userId+'\t'+this.date.getTime().getTime()+'\t'+
//				this.latitude+'\t'+this.longitude+'\t'+this.text
//				+'\t'+this.userName;
//	}
		
}

//class TimeComparator implements Comparator<Tweet>{
//
//	@Override
//	public int compare(Tweet o1, Tweet o2) {
//		// TODO Auto-generated method stub
//		return o1.getDate().compareTo(o2.getDate());
//	}
//	
//}
