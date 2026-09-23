-- Fails when an advantage event reaches silver with the upstream misspelling 'recieved' (KNOWN_ISSUES #11).
select advantage_strategy_key, event_type
from {{ ref('advantage_strategy') }}
where event_type like '%recieved%'
