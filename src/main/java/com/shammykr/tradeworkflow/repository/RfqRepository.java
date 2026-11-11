package com.shammykr.tradeworkflow.repository;

import com.shammykr.tradeworkflow.domain.Rfq;
import com.shammykr.tradeworkflow.domain.RfqStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RfqRepository extends JpaRepository<Rfq, UUID> {

  Page<Rfq> findByStatus(RfqStatus status, Pageable pageable);

  /** RFQs still QUOTED whose active quote has expired. */
  @Query(
      """
      select distinct q.rfq.id from Quote q
      where q.status = com.shammykr.tradeworkflow.domain.QuoteStatus.ACTIVE
        and q.rfq.status = com.shammykr.tradeworkflow.domain.RfqStatus.QUOTED
        and q.validUntil <= :now
      """)
  List<UUID> findIdsWithExpiredQuotes(@Param("now") Instant now);

  long countByStatus(RfqStatus status);
}
