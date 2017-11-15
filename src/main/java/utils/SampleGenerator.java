package utils;

import java.util.Random;

public class SampleGenerator {
	static SampleGenerator instance = null;

	public static SampleGenerator getInstance(){
		if (instance == null)
			instance = new SampleGenerator();
		return instance;
	}
	public String getString(int len){
		String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		StringBuilder salt = new StringBuilder();
		Random rnd = new Random();
		while (salt.length() < len) { // length of the random string.
			int index = (int) (rnd.nextFloat() * SALTCHARS.length());
			salt.append(SALTCHARS.charAt(index));
		}
		String saltStr = salt.toString();
		return saltStr;		
	}

	public String getNumber(int len){
		String SALTCHARS = "1234567890";
		StringBuilder salt = new StringBuilder();
		Random rnd = new Random();
		while (salt.length() < len) { // length of the random string.
			int index = (int) (rnd.nextFloat() * SALTCHARS.length());
			salt.append(SALTCHARS.charAt(index));
		}
		String saltStr = salt.toString();
		return saltStr;		
	}

	public String getPhone(){
		String phone = "+"+getNumber(2)
			+"-"+getNumber(4)
			+"-"+getNumber(4);
		return phone;

	}

}
