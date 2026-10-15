# Reflection

The hardest faults to catch are the subtle, near-baseline deviations. A single
metric can remain inside its clean range even when the event is faulty, so the
defense checks the complete response returned by the relevant toolkit call.
Contract violations are especially important because schema and type failures
are represented explicitly in the violations list rather than as a numeric
threshold. Lineage faults also require checking both runtime and graph shape,
while AI-infrastructure faults require separate checks for drift and staleness.

The current strategy makes one metered call per event. This gives every event
a chance to be evaluated while avoiding redundant calls and keeping cost under
control. The tradeoff is that the detector relies on the published measurements
and baseline thresholds; it does not spend additional calls to confirm a
borderline result. With another pass, I would use practice results to calibrate
small margins around each threshold and combine correlated borderline signals,
while preserving the one-call-per-event budget. I would also prioritize any
pillar that shows a high false-negative rate in private-like validation, rather
than alerting broadly and increasing false positives.
