package Spark3;

import java.io.Serializable;
import java.util.Comparator;

public interface Comparatore<T> extends Comparator<T>, Serializable {
	static <T> Comparatore<T> compara(Comparatore<T> comparatore) {
		return comparatore;
	
	}
}