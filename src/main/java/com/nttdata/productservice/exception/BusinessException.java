package com.nttdata.productservice.exception;

/**
 * Custom exception for business rule violations.
 * Used when operations violate banking regulations.
 */
public class BusinessException extends RuntimeException {

    private String errorCode;

    /**
     * Constructor with message.
     * @param message the error message
     */
    public BusinessException(String message) {
        super(message);
        this.errorCode = "BUSINESS_ERROR";
    }

    /**
     * Constructor with message and error code.
     * @param message the error message
     * @param errorCode the error code
     */
    public BusinessException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Constructor with message and cause.
     * @param message the error message
     * @param cause the cause
     */
    public BusinessException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "BUSINESS_ERROR";
    }

    /**
     * Get the error code.
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Set the error code.
     * @param errorCode the error code
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
}

