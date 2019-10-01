package cs555.util;

public enum Direction {
	
	CLOCKWISE ("R"), COUNTER_CLOCKWISE("L");
	
	private String direction;
	
	private Direction(String direction) {
		this.direction = direction;
	}

	public String getDirection() {
		return this.direction;
	}
	
}
