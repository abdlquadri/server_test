package com.company;

/**
 * Created by SalmonKiller on 11/17/16.
 */
public class RequiredParamsMissingException extends RuntimeException {
    public RequiredParamsMissingException() {}

    public RequiredParamsMissingException(RuntimeException e) {
        super(e);
    }

    public RequiredParamsMissingException(String message) {
        super(message);
    }

}
