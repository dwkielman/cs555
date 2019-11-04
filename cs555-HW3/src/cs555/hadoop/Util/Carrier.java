package cs555.hadoop.Util;

public class Carrier {
	
	private String code;
	private String description;
	
	public Carrier(String code, String description) {
		super();
		this.code = code;
		this.description = description;
	}

	public String getCode() {
		return code;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Carrier) {
			Carrier carrier = (Carrier) obj;
			if (this.code.equals(carrier.getCode())) {
				return true;
			}
		}
		return false;
	}
}
