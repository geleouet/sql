package fr.egaetan.sql.exception;

public class TableNameSpecifiedMoreThanOnce extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public TableNameSpecifiedMoreThanOnce(String message) {
		super(message);
	}

}
