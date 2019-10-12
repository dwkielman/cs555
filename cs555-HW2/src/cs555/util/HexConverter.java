package cs555.util;

public class HexConverter {

	/**
	* This method converts a set of bytes into a Hexadecimal representation.
	*
	* @param buf
	* @return
	*/
	public static String convertBytesToHex(byte[] buf) {
		
		StringBuffer strBuf = new StringBuffer();
		
		for (int i = 0; i < buf.length; i++) {
				int byteValue = (int) buf[i] & 0xff;
				
				if (byteValue <= 15) {
					strBuf.append("0");
				}
	
				strBuf.append(Integer.toString(byteValue, 16));
	
		}
	
		return strBuf.toString();
	}
	
	
	/**
	* This method converts a specified hexadecimal String into a set of bytes.
	*
	* @param hexString
	* @return
	*/
	public static byte[] convertHexToBytes(String hexString) {
	
		int size = hexString.length();
		byte[] buf = new byte[size / 2];
		int j = 0;
	
		for (int i = 0; i < size; i++) {
			String a = hexString.substring(i, i + 2);
			int valA = Integer.parseInt(a, 16);
	
			i++;
			buf[j] = (byte) valA;
			j++;
		}
	
		return buf;
	}
}
