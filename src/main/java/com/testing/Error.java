package com.testing;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;


public class Error implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 880613280087594060L;

	@JsonProperty
	private String code;

	@JsonProperty
	private String message;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
