-ifndef (GENEXPIRE_HRL).
-define (GENEXPIRE_HRL, true).

% limit =
%   { bytes_per_box, MaxBytesPerBox::integer () }
% | { bytes_per_box, MaxBytesPerBox::integer (), UserExpire::function() }
% | { entries_per_box, MaxEntriesPerBox::integer () }
% | { entries_per_box, MaxEntriesPerBox::integer (), UserExpire::function() }
%
% where UserExpire (atom()) -> boolean() returns true if more entries
% should be expired from the given table

-record (expirespecv2, { table, limit }).

% depricated ... do not use in new code

-record (expirespec, { table, max_bytes_per_box }).

-endif.
