# Namespace benchmark medians (2026-04-16)

- steady paginated: secondary-index 231.145 us, repairing-read-plane 35.881 us, strict-read-plane 35.414 us
- mixed paginated: secondary-index 589.396 us, repairing-read-plane 1630.791 us, repair-then-strict 1842.048 us
- deep descendants: flat-scan 476.296 us, secondary-index 27.152 us, read-plane 1.749 us
- repair cost: cold-bootstrap 1602.500 us, hot-page-fold 318.083 us, hot-page-split 310.167 us, verify 9448.208 us, materialize 503505.791 us, rebuild 522604.667 us
