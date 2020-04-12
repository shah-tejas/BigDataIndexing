package com.tejas.bdidemo.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.UNAUTHORIZED)
public class NotAuthorisedException extends RuntimeException {
    public NotAuthorisedException(String message) {
        super(message);
    }
}
