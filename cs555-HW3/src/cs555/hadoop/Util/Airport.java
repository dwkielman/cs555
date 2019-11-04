package cs555.hadoop.Util;

public class Airport {

	private String code;
	private String name;
	private String city;
	
	public Airport(String code, String name, String city) {
		this.code = code;
		this.name = name;
		this.city = city;
	}

	public String getCode() {
		return code;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Airport) {
			Airport airport = (Airport) obj;
			if (this.code.equals(airport.getCode())) {
				return true;
			}
		}
		return false;
	}
}
