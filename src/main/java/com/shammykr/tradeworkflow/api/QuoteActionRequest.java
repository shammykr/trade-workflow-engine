package com.shammykr.tradeworkflow.api;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record QuoteActionRequest(@NotNull UUID quoteId) {}
