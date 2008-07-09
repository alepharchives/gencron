-ifndef (GENEXPIRE_HRL).
-define (GENEXPIRE_HRL, true).

% limit = { bytes_per_box, MaxBytesPerBox::integer () } | 
%         { entries_per_box, MaxEntriesPerBox::integer () }

-record (expirespecv2, { table, limit }).

% depricated ... do not use in new code

-record (expirespec, { table, max_bytes_per_box }).

-endif.
