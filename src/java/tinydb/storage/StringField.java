package tinydb.storage;

import tinydb.common.Type;
import tinydb.execution.Predicate;

import java.io.*;

/**
 * 存储固定长度的单个字符串的 Field 实例
 */
public class StringField implements Field {

	private static final long serialVersionUID = 1L;

	private final String value;
	private final int maxSize;

	public String getValue() {
		return value;
	}

	/**
	 * 
	 * @param s The value of this field.
	 * @param maxSize the maximum size of this string
	 */
	public StringField(String s, int maxSize) {
		this.maxSize = maxSize;

		if (s.length() > maxSize) {
			value = s.substring(0, maxSize);
		} else {
			value = s;
		}
	}

	@Override
	public String toString() {
		return value;
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	public boolean equals(Object field) {
	    if (!(field instanceof StringField)) {
			return false;
		}
		return ((StringField) field).value.equals(value);
	}

	/**
	 * 将此字符串写入 dos。始终将 maxSize + 4 个字节写入传入的 dos。
	 * 前四个字节是字符串长度，接下来的字节是字符串，余数用 0 填充到 maxSize
	 * @param dos
	 */
	@Override
	public void serialize(DataOutputStream dos) throws IOException {
		String s = value;
		int overflow = maxSize - s.length();
		if (overflow < 0) {
            s = s.substring(0, maxSize);
		}
		dos.writeInt(s.length());
		dos.writeBytes(s);
		while (overflow-- > 0) {
			dos.write((byte) 0);
		}
	}

	/**
	 * 将指定字段与该字段的值进行compare
	 * return semantics are as specified by field.compare
	 * 
	 * @throws if val is not a StringField
	 */
	@Override
	public boolean compare(Predicate.Op op, Field val) {

		StringField iVal = (StringField) val;
		int cmpVal = value.compareTo(iVal.value);

		switch (op) {
		case EQUALS:
			return cmpVal == 0;

		case NOT_EQUALS:
			return cmpVal != 0;

		case GREATER_THAN:
			return cmpVal > 0;

		case GREATER_THAN_OR_EQ:
			return cmpVal >= 0;

		case LESS_THAN:
			return cmpVal < 0;

		case LESS_THAN_OR_EQ:
			return cmpVal <= 0;

		case LIKE:
			return value.contains(iVal.value);
		}

		return false;
	}

	/**
	 * @return the Type for this Field
	 */
	@Override
	public Type getType() {

		return Type.STRING_TYPE;
	}
}
