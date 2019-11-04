package cs555.hadoop.Util;

public class Plane {
	
	private String tailNumber;
	private int year;
	
	public Plane(String tailNumber, int year) {
		this.tailNumber = tailNumber;
		this.year = year;
	}
	
	public String getTailNumber() {
		return tailNumber;
	}
	
	public int getYear() {
		return year;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Plane) {
			Plane plane = (Plane) obj;
			if (this.tailNumber.equals(plane.getTailNumber())) {
				return true;
			}
		}
		return false;
	}
	
}
