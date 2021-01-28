package com.testing;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class ExceptionController {
	
	@ExceptionHandler(value = SystemException.class)
	public ResponseEntity<Error> exception(SystemException systemException){
		Error error = new Error();
		error.setCode("100");
		error.setMessage(systemException.getMessage());
		return ResponseEntity.badRequest().body(error);
	}

}
