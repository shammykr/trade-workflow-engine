package com.shammykr.tradeworkflow.api;

import com.shammykr.tradeworkflow.domain.Rfq;
import com.shammykr.tradeworkflow.domain.RfqStatus;
import com.shammykr.tradeworkflow.repository.RfqRepository;
import com.shammykr.tradeworkflow.workflow.RfqNotFoundException;
import com.shammykr.tradeworkflow.workflow.RfqWorkflowService;
import com.shammykr.tradeworkflow.workflow.SubmitRfqCommand;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * RFQ workflow API. Responses carry the RFQ version as an {@code ETag}; clients can send it back as
 * {@code If-Match} on accept/decline so an action based on stale state fails with 412 instead of
 * overriding a colleague's change. The acting desk user is taken from {@code X-Desk-User}.
 */
@RestController
@RequestMapping("/api/rfqs")
public class RfqController {

  private final RfqWorkflowService workflow;
  private final RfqRepository rfqs;

  public RfqController(RfqWorkflowService workflow, RfqRepository rfqs) {
    this.workflow = workflow;
    this.rfqs = rfqs;
  }

  @PostMapping
  public ResponseEntity<RfqResponse> submit(
      @Valid @RequestBody SubmitRfqRequest req,
      @RequestHeader(value = "X-Desk-User", defaultValue = "api") String actor) {
    Rfq rfq =
        workflow.submit(
            new SubmitRfqCommand(
                req.clientId(),
                req.underlier(),
                req.productType(),
                req.notional(),
                req.currency(),
                req.maturityMonths()),
            actor);
    return withEtag(ResponseEntity.created(URI.create("/api/rfqs/" + rfq.getId())), rfq);
  }

  @GetMapping("/{id}")
  @Transactional(readOnly = true)
  public ResponseEntity<RfqResponse> get(@PathVariable UUID id) {
    Rfq rfq = rfqs.findById(id).orElseThrow(() -> new RfqNotFoundException(id));
    return withEtag(ResponseEntity.ok(), rfq);
  }

  @GetMapping
  @Transactional(readOnly = true)
  public Page<RfqResponse> list(
      @RequestParam(required = false) RfqStatus status,
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "20") int size) {
    PageRequest pageable =
        PageRequest.of(page, Math.min(size, 200), Sort.by(Sort.Direction.DESC, "createdAt"));
    Page<Rfq> result =
        status == null ? rfqs.findAll(pageable) : rfqs.findByStatus(status, pageable);
    return result.map(RfqResponse::of);
  }

  @GetMapping("/{id}/transitions")
  @Transactional(readOnly = true)
  public List<RfqResponse.TransitionView> transitions(@PathVariable UUID id) {
    Rfq rfq = rfqs.findById(id).orElseThrow(() -> new RfqNotFoundException(id));
    return rfq.getTransitions().stream().map(RfqResponse.TransitionView::of).toList();
  }

  @PostMapping("/{id}/accept")
  public ResponseEntity<RfqResponse> accept(
      @PathVariable UUID id,
      @Valid @RequestBody QuoteActionRequest req,
      @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
      @RequestHeader(value = "X-Desk-User", defaultValue = "api") String actor) {
    Rfq rfq = workflow.accept(id, req.quoteId(), parseVersion(ifMatch), actor);
    return withEtag(ResponseEntity.ok(), rfq);
  }

  @PostMapping("/{id}/decline")
  public ResponseEntity<RfqResponse> decline(
      @PathVariable UUID id,
      @Valid @RequestBody QuoteActionRequest req,
      @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
      @RequestHeader(value = "X-Desk-User", defaultValue = "api") String actor) {
    Rfq rfq = workflow.decline(id, req.quoteId(), parseVersion(ifMatch), actor);
    return withEtag(ResponseEntity.ok(), rfq);
  }

  private static ResponseEntity<RfqResponse> withEtag(ResponseEntity.BodyBuilder builder, Rfq rfq) {
    return builder.eTag(Long.toString(rfq.getVersion())).body(RfqResponse.of(rfq));
  }

  /** Accepts {@code "3"}, {@code 3} or {@code W/"3"}. */
  static Long parseVersion(String ifMatch) {
    if (ifMatch == null || ifMatch.isBlank()) {
      return null;
    }
    String v = ifMatch.trim();
    if (v.startsWith("W/")) {
      v = v.substring(2);
    }
    v = v.replace("\"", "");
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("If-Match must be an RFQ version, got " + ifMatch);
    }
  }
}
