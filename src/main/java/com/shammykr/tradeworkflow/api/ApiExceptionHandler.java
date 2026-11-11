package com.shammykr.tradeworkflow.api;

import com.shammykr.tradeworkflow.domain.InvalidTransitionException;
import com.shammykr.tradeworkflow.workflow.QuoteExpiredException;
import com.shammykr.tradeworkflow.workflow.RfqNotFoundException;
import com.shammykr.tradeworkflow.workflow.StaleVersionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Maps workflow failures to RFC 9457 problem details. */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

  @ExceptionHandler(RfqNotFoundException.class)
  ProblemDetail notFound(RfqNotFoundException e) {
    return problem(HttpStatus.NOT_FOUND, "RFQ not found", e.getMessage());
  }

  @ExceptionHandler(InvalidTransitionException.class)
  ProblemDetail invalidTransition(InvalidTransitionException e) {
    return problem(HttpStatus.CONFLICT, "Invalid state transition", e.getMessage());
  }

  @ExceptionHandler(QuoteExpiredException.class)
  ProblemDetail expired(QuoteExpiredException e) {
    return problem(HttpStatus.CONFLICT, "Quote expired", e.getMessage());
  }

  /** Another request changed the RFQ between our read and our write (JPA @Version). */
  @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
  ProblemDetail concurrentUpdate(ObjectOptimisticLockingFailureException e) {
    return problem(
        HttpStatus.CONFLICT,
        "Concurrent update",
        "the RFQ was modified by another request; reload and retry");
  }

  @ExceptionHandler(StaleVersionException.class)
  ProblemDetail stale(StaleVersionException e) {
    ProblemDetail p = problem(HttpStatus.PRECONDITION_FAILED, "Stale version", e.getMessage());
    p.setProperty("currentVersion", e.getCurrentVersion());
    return p;
  }

  @ExceptionHandler(IllegalArgumentException.class)
  ProblemDetail badRequest(IllegalArgumentException e) {
    return problem(HttpStatus.BAD_REQUEST, "Bad request", e.getMessage());
  }

  private static ProblemDetail problem(HttpStatus status, String title, String detail) {
    ProblemDetail p = ProblemDetail.forStatusAndDetail(status, detail);
    p.setTitle(title);
    return p;
  }
}
